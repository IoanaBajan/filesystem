import logging

import globals
from userCommands import mkdir, rm, ls, cat, create, mv_file, cd, addFile, chmod, getPermissions


def main():
    globals.initialize()
    while True:
        command = input('enter command\n')

        if command.__contains__('ls'):
            ls(command)
        elif command.__contains__('touch '):
            create(command)
        elif command.__contains__('cat '):
            cat(command)
        elif command.__contains__('mv '):
            mv_file(command)
        elif command.__contains__('rm '):
            rm(command)
        elif command.__contains__('rmdir '):
            rm(command)
        elif command.__contains__('mkdir '):
            mkdir(command)
        elif command.__contains__('add '):
            addFile(command)
        elif command.__contains__('cd '):
            cd(command)
        elif command.__contains__('chmod '):
            chmod(command)
        elif command.__contains__('permissions '):
            getPermissions(command)
        elif command.__contains__('history'):
            print(globals.history)
        elif command.__contains__('pwd'):
            print(globals.current_dir)
        else:
            logging.error('not a valid choice')
            globals.history = globals.history.append({'failure': command, 'success': '-'}, ignore_index=True)
if __name__ == "__main__":
    main()
