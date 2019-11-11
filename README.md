# PredicAd
A Machine Learning program in Scala. Project made in IG5.
## Dependencies
In order to work, the program requires the following dependencies to be installed
* SBT version 1.3.3
* Scala version 2.11.6
* Spark version 2.3.0 (Core, SQL an MLlib)
* Java 8

**Note :** Your current Java version must be 8 for the program to work. If you have multiple version, you need to switch to Java 8.

## How to run it
* Clone the repository
* Go to the root of the projet and compile it with the following command : 
```
sbt package
```
* Make the script executable by typing : 
```
chmod +x script.sh
```
* Type : 
```
./script.sh <your test JSON path>
```

The result will be written in a csv file named "resultPredicad.csv".

**Note :** The model is already given in the repository. If you want to recreate your own model, delete the "save" folder located 
in the root of the project and type : 
```
./script.sh <your test JSON path> <your train JSON path>
```

However, this can take some time (up to one hour for a JSON containing 1 000 000 data).

## Metrics (for a dataset of 1 000 000 data)
* **Accuracy :** 98.39 %
* **Precision (false) :** 98.59 %
* **Precision (true) :** 85.10 %
* **Recall (false) :** 99.78 %
* **Recall (true) :** 46.46 %
* **True Positives :** 12 066
* **False Positives :** 2 112
* **True Negatives :** 971 760
* **False Negatives :** 13 903
