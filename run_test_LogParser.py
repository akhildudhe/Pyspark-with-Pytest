# Experiment code for passing arguments from command line

import pytest
import sys

def main():
    # extract your arg here
    print('argument'+sys.argv[2])
    pytest.main([sys.argv[1]])

if __name__ == '__main__':
    main()