#!/usr/bin/env python3
import signal
import subprocess
import sys


def main():
    # Launch caffeinate to inhibit all sleep modes (-ddisplay, -iidle, -mdisk, -ssystem)
    proc = subprocess.Popen(["caffeinate", "-d", "-i", "-m", "-s"])
    print(" Keeping Mac awake – press Ctrl+C to exit.")

    # When we get Ctrl+C, terminate caffeinate and exit cleanly.
    def _cleanup(signum, frame):
        proc.terminate()
        print("\n☾ Sleep behavior restored. Goodbye.")
        sys.exit(0)

    signal.signal(signal.SIGINT, _cleanup)
    # Wait until caffeinate exits (which will be when we send it SIGTERM)
    proc.wait()


if __name__ == "__main__":
    main()
