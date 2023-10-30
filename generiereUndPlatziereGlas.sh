rm ./*.jar
#rm ScenarioCreator.Generation/target/*-shaded.jar
mvn clean package -Dmaven.test.skip
cp ScenarioCreator.Generation/target/*-shaded.jar ./
#java --enable-preview -jar ./*.jar --körnerkissen --notgd -av $PWD/out
rm -rf /home/mrobohm/Downloads/korn/*
java -jar --enable-preview ./*.jar --körnerkissen -av /home/mrobohm/Downloads/korn -ev ~/Weichware/Instanzdatenholer/daten --samen $1 --einzelausführung RenameColumn
