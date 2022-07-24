# ScenarioCreator
## Getting knowledge base
This project uses GermaNet and WordNet, thus needs to have access to those files. First you need to create the directories
```
XYZ/ScenarioCreator/src/main/resources/wordnet
```
and
```
XYZ/ScenarioCreator/src/main/resources/germanet
```
The files of the WordNet corpus can be downloaded [here](https://wordnetcode.princeton.edu/3.0/WordNet-3.0.tar.gz). 
You need to place the `dict`-folder into the first created directory. 
The second created directory has to be filled with the files of the GermaNet corpus, which you have to obtain by yourself. 
The files should reside in a folder named `GN_V160` in the second created directory. 
To get the translation feature working, you have to download [ILI-records](https://www.sfs.uni-tuebingen.de/GermaNet/documents/ili/GN-V16.0-ILI-Mappings-DE-EN-XML.zip) and follow its instruction in the `README` file.
