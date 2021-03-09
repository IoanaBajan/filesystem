import logging
import pandas as pd
import globals
from userCommands import mkdir, rm, ls, cat, create, mv_file, pr, cd

def main():

    globals.initialize()
    # logging.info(globals.current_dir)
    while True:
        command = input('enter command\n')

        if command.__contains__('ls'):
            ls(command)

        elif command.__contains__('nano '):
            create(command)
        # shows input of file
        elif command.__contains__('cat '):
           cat(command)
        elif command.__contains__('mv '):
            logging.debug("rename key")
            mv_file(command)
        elif command.__contains__('rm '):
            rm(command)
        elif command.__contains__('mkdir '):
            mkdir(command)

        elif command.__contains__('pr '):
            pr(command,globals.history['success'].iloc[-1])

        elif command.__contains__('cd '):
           cd(command)
        elif command.__contains__('history'):
           print(globals.history)
        elif command.__contains__('pwd'):
            print(globals.current_dir)
        else:
            logging.error('not a valid choice')
            globals.history = globals.history.append({'failure': command, 'success': '-'}, ignore_index=True)


if __name__ == "__main__":
    main()
