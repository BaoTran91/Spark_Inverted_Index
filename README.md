##Quick set-up and run.
1) open project in InteliJ and allow sbt to build and get dependencies.
2) run /src/main/libraries_index.scala file

##Through shell
1) cd into home directory
2) execute "sbt" to enter sbt shell
3) execute "run" inside sbt shell to run spark job
4) the dictionary will be writen out to the dictionary folder and the final index in the inverted index folder.

The spark job (src/main/scala/libraries_index) is writen in scale and it utilizes the spark DataFrame.
