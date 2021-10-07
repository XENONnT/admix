import sys
from tqdm import tqdm
from . import rucio
from .utils import make_did


def get_dtype_status(run, dtype, hash, rse=None):
    did = make_did(run, dtype, hash)
    if rse:
        rules = rucio.list_rules(did, rse_expression=rse)
    else:
        rules = rucio.list_rules(did)

    title = f"| {'RSE':^20s} | {'STATUS':^15s} | {'PROGRESS':^25s} |"
    suptitle = f"| {did:^66s} |"
    overline_bar = tqdm(desc="-" * len(title), bar_format="{desc}", file=sys.stdout)
    overline_bar.close()
    suptitle_bar = tqdm(desc=suptitle, bar_format="{desc}", file=sys.stdout)
    suptitle_bar.close()
    underline_bar = tqdm(desc="-" * len(title), bar_format="{desc}", file=sys.stdout)
    underline_bar.close()
    title_bar = tqdm(desc=title, bar_format="{desc}", file=sys.stdout)
    title_bar.close()
    underline_bar = tqdm(desc="-"*len(title), bar_format="{desc}", file=sys.stdout)
    underline_bar.close()
    for i, rule in enumerate(rules):
        num_ok = rule['locks_ok_cnt']
        num_replicating = rule['locks_replicating_cnt']
        num_stuck = rule['locks_stuck_cnt']
        total = num_ok + num_replicating + num_stuck

        rse = rule['rse_expression']
        bar = tqdm(total=total,
                   desc=f"| {rse:^20s} | {rule['state']:^15s} |",
                   bar_format="{desc} {percentage:3.0f}%|{bar}|{n_fmt}/{total_fmt}|",
                   ncols=70,
                   file=sys.stdout
                   )
        bar.update(num_ok)
        bar.close()
    underline_bar = tqdm(desc="-" * len(title), bar_format="{desc}", file=sys.stdout)
    underline_bar.close()
    # blank space for readability
    print()


def get_run_status(run, dtype=None, rse=None):
    scope = f"xnt_{run:06d}"
    datasets = rucio.list_datasets(scope)
    for d in datasets:
        dt, hsh = d.split('-')
        if dtype:
            if dt == dtype:
                get_dtype_status(run, dt, hsh, rse=rse)
        else:
            get_dtype_status(run, dt, hsh, rse=rse)
