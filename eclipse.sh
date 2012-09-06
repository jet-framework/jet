for BACKEND in crunch spark scoobi
do
	cd $BACKEND
	sbt eclipse
	#rmdir --ignore-fail-on-non-empty src/test/scala src/test/java 
	for DIR in src/test/scala src/test/java src/main/java
	do
	if [ "$(ls -A ./$DIR)" ]; then
		echo "$DIR is not empty"
	else
		rmdir $DIR
	     	#echo "Take action $DIR is not Empty"
   		cp .classpath .classpath.gen
		mv .classpath .classpath.bak
		cat .classpath.bak | grep -Ev "$DIR" > .classpath
		sed -i 's|output=\"target.*classes\"|output="bin"|g' .classpath
	fi
	done
	cd ..
done
