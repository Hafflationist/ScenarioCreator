mvn package -Dmaven.test.skip
cp ScenarioCreator.Generation/target/*.jar ./
java --enable-preview -jar ./*.jar --körnerkissen --notgd -av out
