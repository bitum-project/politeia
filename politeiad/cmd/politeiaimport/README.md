# politeiaimport

`politeiaimport` is a tool to import data from a public politeia repo such as
the [bitum proposals repo](https://github.com/bitum-proposals/mainnet/).

## Usage 

Install `politeiaimport`.

    $ go install $GOPATH/src/github.com/bitum-project/politeia/politeiad/cmd/politeiaimport

Clone the repo you want to import.

    $ git clone https://github.com/bitum-proposals/mainnet.git ~/mainnet

Import the repo data.  If you're importing testnet data you must use the 
`--testnet` flag.

    $ politeiaimport ~/mainnet 
    You are about to delete     : ~/.politeiad/data/mainnet
    It will be replaced with    : ~/mainnet
    Continue? (n/no/y/yes) [no] : yes
    Walking import directory...
    Done!

`politeiaimport` replaces the existing unvetted and vetted repos with the data
from the import directory.  The journal files are then recreated using the
import data.  The git history of the import directory is kept intact.
