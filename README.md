# CSV Splitter

Split large CSV files into chunks of n row size using Pandas. 

## Arguments

Scrips uses these arguments:

* -i Absolute path to input CSV
* -o Absolute path to folder where CSV chunks will be stored
* -r Row size of the chunks
* -s Separator character used in input CSV


Example:

```sh
main.py -i Reg_64ms.csv -s ; -o /tmp -r 30000
```
