mvn package -Dmaven.test.skip
cp ScenarioCreator.Generation/target/*-shaded.jar ./
java --enable-preview -jar ./*.jar --körnerkissen --notgd -av $PWD/out
