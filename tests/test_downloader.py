import os
import tempfile
import unittest
from unittest.mock import patch

from admix.downloader import determine_rse, download, download_1t


class TestDetermineRSE(unittest.TestCase):
    """Unit tests for admix.downloader.determine_rse."""

    def _determine(self, rse_list, fqdn="unknown.example.org", glidein=None):
        env = {}
        if glidein is not None:
            env["GLIDEIN_Country"] = glidein
        with patch.dict(os.environ, env, clear=True):
            with patch("admix.downloader.socket.getfqdn", return_value=fqdn):
                return determine_rse(rse_list)

    def test_empty_inputs_return_none(self):
        for empty in ([], (), set()):
            for fqdn, glidein in (
                ("midway3-0001.rcc.local", None),
                ("ccage01.in2p3.fr", None),
                ("worker.example.org", "EUROPE"),
                ("worker.example.org", None),
            ):
                with self.subTest(empty=empty, fqdn=fqdn, glidein=glidein):
                    self.assertIsNone(
                        self._determine(empty, fqdn=fqdn, glidein=glidein)
                    )

    def test_rcc_host_preferences_and_fallbacks(self):
        for fqdn in ("midway3-0001.rcc.local", "beagle3-tbd1.rcc.local"):
            with self.subTest(fqdn=fqdn):
                # Nearby US disk beats EU disk
                self.assertEqual(
                    self._determine(["CNAF_USERDISK", "UC_OSG_USERDISK"], fqdn),
                    "UC_OSG_USERDISK",
                )
                self.assertEqual(
                    self._determine(["UC_OSG_USERDISK", "UC_DALI_USERDISK"], fqdn),
                    "UC_DALI_USERDISK",
                )

                # EU disk beats tape regardless of list order
                self.assertEqual(
                    self._determine(["CCIN2P3_USERDISK", "CNAF_USERDISK"], fqdn),
                    "CNAF_USERDISK",
                )
                self.assertEqual(
                    self._determine(["CNAF_USERDISK", "CCIN2P3_USERDISK"], fqdn),
                    "CNAF_USERDISK",
                )
                self.assertEqual(
                    self._determine(
                        ["CCIN2P3_USERDISK", "CCIN2P32_USERDISK"], fqdn
                    ),
                    "CCIN2P32_USERDISK",
                )

                # Unlisted non-tape RSEs beat tape RSEs
                self.assertEqual(
                    self._determine(
                        ["SURFSARA_USERDISK", "UC_MIDWAY_USERDISK"], fqdn
                    ),
                    "UC_MIDWAY_USERDISK",
                )
                self.assertEqual(
                    self._determine(["CCIN2P3_USERDISK", "LNGS_USERDISK"], fqdn),
                    "LNGS_USERDISK",
                )
                self.assertEqual(
                    self._determine(
                        ["CNAF_TAPE4_USERDISK", "FUTURE_DISK_USERDISK"], fqdn
                    ),
                    "FUTURE_DISK_USERDISK",
                )

                # Tape-only datasets resolve instead of returning None
                self.assertEqual(
                    self._determine(["CCIN2P3_USERDISK"], fqdn),
                    "CCIN2P3_USERDISK",
                )
                self.assertEqual(
                    self._determine(["SURFSARA_USERDISK"], fqdn),
                    "SURFSARA_USERDISK",
                )
                self.assertEqual(
                    self._determine(["CNAF_TAPE3_USERDISK"], fqdn),
                    "CNAF_TAPE3_USERDISK",
                )

    def test_other_hosts(self):
        # in2p3 prefers CCIN2P32_USERDISK over CCIN2P3_USERDISK, resolves tape-only
        self.assertEqual(
            self._determine(
                ["CCIN2P3_USERDISK", "CCIN2P32_USERDISK"],
                fqdn="ccwsge01.in2p3.fr",
            ),
            "CCIN2P32_USERDISK",
        )
        self.assertEqual(
            self._determine(["CCIN2P3_USERDISK"], fqdn="ccwsge01.in2p3.fr"),
            "CCIN2P3_USERDISK",
        )

        # sdsc prefers SDSC_NSDF, falls back to EU disk over tape, resolves tape-only
        self.assertEqual(
            self._determine(
                ["UC_OSG_USERDISK", "SDSC_NSDF_USERDISK"],
                fqdn="expanse.sdsc.edu",
            ),
            "SDSC_NSDF_USERDISK",
        )
        self.assertEqual(
            self._determine(
                ["CCIN2P3_USERDISK", "CNAF_USERDISK"],
                fqdn="expanse.sdsc.edu",
            ),
            "CNAF_USERDISK",
        )
        self.assertEqual(
            self._determine(["CCIN2P3_USERDISK"], fqdn="expanse.sdsc.edu"),
            "CCIN2P3_USERDISK",
        )

        # nikhef and surf preserve local preference and resolve unlisted/tape
        self.assertEqual(
            self._determine(
                ["CNAF_USERDISK", "NIKHEF2_USERDISK"],
                fqdn="wn.nikhef.nl",
            ),
            "NIKHEF2_USERDISK",
        )
        self.assertEqual(
            self._determine(
                ["CNAF_USERDISK", "SURFSARA2_USERDISK"],
                fqdn="wn.surf.nl",
            ),
            "SURFSARA2_USERDISK",
        )

    def test_glidein_countries(self):
        for country in ("EUROPE", "FR"):
            with self.subTest(country=country):
                self.assertEqual(
                    self._determine(
                        ["CCIN2P3_USERDISK", "CNAF_USERDISK"],
                        glidein=country,
                    ),
                    "CNAF_USERDISK",
                )
                self.assertEqual(
                    self._determine(["CCIN2P3_USERDISK"], glidein=country),
                    "CCIN2P3_USERDISK",
                )
                self.assertEqual(
                    self._determine(["LNGS_USERDISK"], glidein=country),
                    "LNGS_USERDISK",
                )
                self.assertEqual(
                    self._determine(["CNAF_TAPE3_USERDISK"], glidein=country),
                    "CNAF_TAPE3_USERDISK",
                )

        self.assertEqual(
            self._determine(
                ["CNAF_USERDISK", "UC_OSG_USERDISK"],
                glidein="US",
            ),
            "UC_OSG_USERDISK",
        )
        self.assertEqual(
            self._determine(["CCIN2P3_USERDISK"], glidein="US"),
            "CCIN2P3_USERDISK",
        )

    def test_determinism_with_reversed_list_and_set(self):
        r1 = self._determine(
            ["CCIN2P3_USERDISK", "LNGS_USERDISK"],
            fqdn="midway3-0001.rcc.local",
        )
        r2 = self._determine(
            ["LNGS_USERDISK", "CCIN2P3_USERDISK"],
            fqdn="midway3-0001.rcc.local",
        )
        r3 = self._determine(
            {"CCIN2P3_USERDISK", "LNGS_USERDISK"},
            fqdn="midway3-0001.rcc.local",
        )
        self.assertEqual((r1, r2, r3), ("LNGS_USERDISK",) * 3)

        t1 = self._determine(
            ["SURFSARA_USERDISK", "CCIN2P3_USERDISK"],
            fqdn="midway3-0001.rcc.local",
        )
        t2 = self._determine(
            {"SURFSARA_USERDISK", "CCIN2P3_USERDISK"},
            fqdn="midway3-0001.rcc.local",
        )
        self.assertEqual((t1, t2), ("CCIN2P3_USERDISK", "CCIN2P3_USERDISK"))


class TestDownloadTriesAndRSE(unittest.TestCase):
    """Unit tests for RSE preservation on tries=1 and retry fallback."""

    def test_download_tries_1_preserves_explicit_rse(self):
        did = "xnt_087864:raw_records-rfzvpzj4mf-metadata.json"
        with tempfile.TemporaryDirectory() as tmpdir:
            dest = os.path.join(
                tmpdir,
                "087864-raw_records-rfzvpzj4mf",
                "raw_records-rfzvpzj4mf-metadata.json",
            )
            with (
                patch("admix.downloader.get_did_type", return_value="FILE"),
                patch(
                    "admix.rucio.list_files",
                    return_value=["raw_records-rfzvpzj4mf-metadata.json"],
                ),
                patch(
                    "admix.downloader.download_dids",
                    return_value=[{"dest_file_paths": [dest]}],
                ) as mock_dl,
            ):
                paths = download(
                    did, location=tmpdir, tries=1, rse="CNAF_USERDISK"
                )

            self.assertEqual(paths, [dest])
            self.assertEqual(
                [c.kwargs["rse"] for c in mock_dl.call_args_list],
                ["CNAF_USERDISK"],
            )

    def test_download_tries_1_preserves_determined_rse(self):
        did = "xnt_087864:raw_records-rfzvpzj4mf"
        with tempfile.TemporaryDirectory() as tmpdir:
            dest = os.path.join(
                tmpdir,
                "087864-raw_records-rfzvpzj4mf",
                "raw_records-rfzvpzj4mf-metadata.json",
            )
            with (
                patch("admix.downloader.get_did_type", return_value="DATASET"),
                patch(
                    "admix.downloader.get_rses",
                    return_value=["CCIN2P3_USERDISK", "CNAF_USERDISK"],
                ),
                patch(
                    "admix.downloader.socket.getfqdn",
                    return_value="midway3-0001.rcc.local",
                ),
                patch(
                    "admix.downloader.download_dids",
                    return_value=[{"dest_file_paths": [dest]}],
                ) as mock_dl,
            ):
                paths = download(
                    did, location=tmpdir, tries=1, only_metadata=True, rse=None
                )

            self.assertEqual(paths, [dest])
            self.assertEqual(
                [c.kwargs["rse"] for c in mock_dl.call_args_list],
                ["CNAF_USERDISK"],
            )

    def test_download_retries_drop_rse_only_on_final_try(self):
        did = "xnt_087864:raw_records-rfzvpzj4mf"
        for tries, expected_rses in (
            (2, ["CNAF_USERDISK", None]),
            (3, ["CNAF_USERDISK", "CNAF_USERDISK", None]),
        ):
            with self.subTest(tries=tries):
                with tempfile.TemporaryDirectory() as tmpdir:
                    dest = os.path.join(
                        tmpdir,
                        "087864-raw_records-rfzvpzj4mf",
                        "raw_records-rfzvpzj4mf-metadata.json",
                    )
                    failures = [RuntimeError(f"fail {i}") for i in range(tries - 1)]
                    side_effect = failures + [[{"dest_file_paths": [dest]}]]
                    with (
                        patch(
                            "admix.downloader.get_did_type",
                            return_value="DATASET",
                        ),
                        patch(
                            "admix.downloader.download_dids",
                            side_effect=side_effect,
                        ) as mock_dl,
                        patch("admix.downloader.time.sleep") as mock_sleep,
                    ):
                        paths = download(
                            did,
                            location=tmpdir,
                            tries=tries,
                            only_metadata=True,
                            rse="CNAF_USERDISK",
                        )

                    self.assertEqual(paths, [dest])
                    self.assertEqual(
                        [c.kwargs["rse"] for c in mock_dl.call_args_list],
                        expected_rses,
                    )
                    self.assertEqual(mock_sleep.call_count, tries - 1)

    def test_download_1t_tries_1_and_retries(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with (
                patch(
                    "admix.downloader.get_did_1t",
                    return_value="x1t_170101_0000:x1t_run",
                ),
                patch(
                    "admix.downloader.download_dids",
                    return_value=[{"dest_file_paths": []}],
                ) as mock_dl,
            ):
                download_1t(
                    100,
                    "processed",
                    location=tmpdir,
                    tries=1,
                    rse="CNAF_USERDISK",
                )
            self.assertEqual(
                [c.kwargs["rse"] for c in mock_dl.call_args_list],
                ["CNAF_USERDISK"],
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            with (
                patch(
                    "admix.downloader.get_did_1t",
                    return_value="x1t_170101_0000:x1t_run",
                ),
                patch(
                    "admix.downloader.download_dids",
                    side_effect=[1, [{"dest_file_paths": []}]],
                ) as mock_dl,
                patch("admix.downloader.time.sleep") as mock_sleep,
            ):
                download_1t(
                    100,
                    "processed",
                    location=tmpdir,
                    tries=2,
                    rse="CNAF_USERDISK",
                )
            self.assertEqual(
                [c.kwargs["rse"] for c in mock_dl.call_args_list],
                ["CNAF_USERDISK", None],
            )
            mock_sleep.assert_called_once()


if __name__ == "__main__":
    unittest.main()
