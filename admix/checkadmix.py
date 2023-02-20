#!/usr/bin/env python

import time
import psutil
from admix.helper.admix_bot import AdmixBot
import textwrap

def CheckAdmix():

    print("----------------")
    print("-- checkadmix --")
    print("----------------")

    print("The admix upload manager activity is being monitored")

    bot = AdmixBot('admix')

    already_sent_alarm = True

    while 1:

        process_is_running = False

        # Getting the most updated status of processes
        for process in psutil.process_iter(['pid', 'name', 'cmdline']):
            for command in process.info['cmdline']:
                if 'admix-upload-manager' in command:
                    process_is_running = True
                    already_sent_alarm = False
        
        if not process_is_running and not already_sent_alarm:
            print("Alert sent")
            already_sent_alarm = True

            bot.send_message(textwrap.dedent("""
            *Alert—Upload manager crashed* <!channel>

            *Following <https://xe1t-wiki.lngs.infn.it/doku.php?id=xenon:xenonnt:analysis:analysis_tools_team:admix:admix_shifters|aDMIX instructions>, take action immediately!*

            """))

        # Wait for 10 seconds
        time.sleep(10)



def main():
    
    try:
        CheckAdmix()
        
    except KeyboardInterrupt:
        exit(0)



if __name__ == "__main__":
    main()
