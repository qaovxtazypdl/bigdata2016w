spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.TrainSpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.train.group_x.txt --model cs489-2016w-qaovxtazypdl-a6-model-group_x
spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.ApplySpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.test.qrels.txt --output cs489-2016w-qaovxtazypdl-a6-test-group_x --model cs489-2016w-qaovxtazypdl-a6-model-group_x

cat cs489-2016w-qaovxtazypdl-a6-model-group_x/* | head
cat cs489-2016w-qaovxtazypdl-a6-test-group_x/* | sort | head -5
./spam_eval.sh cs489-2016w-qaovxtazypdl-a6-test-group_x



spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.TrainSpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.train.group_y.txt --model cs489-2016w-qaovxtazypdl-a6-model-group_y
spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.ApplySpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.test.qrels.txt --output cs489-2016w-qaovxtazypdl-a6-test-group_y --model cs489-2016w-qaovxtazypdl-a6-model-group_y

cat cs489-2016w-qaovxtazypdl-a6-model-group_y/* | head
cat cs489-2016w-qaovxtazypdl-a6-test-group_y/* | sort | head -5
./spam_eval.sh cs489-2016w-qaovxtazypdl-a6-test-group_y




spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.TrainSpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.train.britney.txt --model cs489-2016w-qaovxtazypdl-a6-model-britney
spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.ApplySpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.test.qrels.txt --output cs489-2016w-qaovxtazypdl-a6-test-britney --model cs489-2016w-qaovxtazypdl-a6-model-britney

cat cs489-2016w-qaovxtazypdl-a6-model-britney/* | head
cat cs489-2016w-qaovxtazypdl-a6-test-britney/* | sort | head -5
./spam_eval.sh cs489-2016w-qaovxtazypdl-a6-test-britney





mkdir cs489-2016w-qaovxtazypdl-a6-model-fusion
cp cs489-2016w-qaovxtazypdl-a6-model-group_x/part-00000 cs489-2016w-qaovxtazypdl-a6-model-fusion/part-00000
cp cs489-2016w-qaovxtazypdl-a6-model-group_y/part-00000 cs489-2016w-qaovxtazypdl-a6-model-fusion/part-00001
cp cs489-2016w-qaovxtazypdl-a6-model-britney/part-00000 cs489-2016w-qaovxtazypdl-a6-model-fusion/part-00002



spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.ApplyEnsembleSpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.test.qrels.txt --output cs489-2016w-qaovxtazypdl-a6-test-fusion-average --model cs489-2016w-qaovxtazypdl-a6-model-fusion --method average
cat cs489-2016w-qaovxtazypdl-a6-test-fusion-average/* | sort | head -5
./spam_eval.sh cs489-2016w-qaovxtazypdl-a6-test-fusion-average

spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.ApplyEnsembleSpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.test.qrels.txt --output cs489-2016w-qaovxtazypdl-a6-test-fusion-vote --model cs489-2016w-qaovxtazypdl-a6-model-fusion --method vote
cat cs489-2016w-qaovxtazypdl-a6-test-fusion-vote/* | sort | head -5
./spam_eval.sh cs489-2016w-qaovxtazypdl-a6-test-fusion-vote





cat data/spam/spam.train.group_x.txt data/spam/spam.train.group_y.txt data/spam/spam.train.britney.txt > data/spam/spam.train.all.txt
spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.TrainSpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.train.all.txt --model cs489-2016w-qaovxtazypdl-a6-model-all
spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.ApplySpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.test.qrels.txt --output cs489-2016w-qaovxtazypdl-a6-test-all --model cs489-2016w-qaovxtazypdl-a6-model-all
cat cs489-2016w-qaovxtazypdl-a6-model-all/* | head
cat cs489-2016w-qaovxtazypdl-a6-test-all/* | sort | head -5
./spam_eval.sh cs489-2016w-qaovxtazypdl-a6-test-all





spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.TrainSpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.train.britney.txt --model cs489-2016w-qaovxtazypdl-a6-model-britney-shuffle --shuffle
spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.ApplySpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.test.qrels.txt --output cs489-2016w-qaovxtazypdl-a6-test-britney-shuffle --model cs489-2016w-qaovxtazypdl-a6-model-britney-shuffle
cat cs489-2016w-qaovxtazypdl-a6-model-britney-shuffle/* | head
cat cs489-2016w-qaovxtazypdl-a6-test-britney-shuffle/* | sort | head -5
./spam_eval.sh cs489-2016w-qaovxtazypdl-a6-test-britney-shuffle

spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.TrainSpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.train.britney.txt --model cs489-2016w-qaovxtazypdl-a6-model-britney-shuffle --shuffle
spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.ApplySpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.test.qrels.txt --output cs489-2016w-qaovxtazypdl-a6-test-britney-shuffle --model cs489-2016w-qaovxtazypdl-a6-model-britney-shuffle
cat cs489-2016w-qaovxtazypdl-a6-model-britney-shuffle/* | head
cat cs489-2016w-qaovxtazypdl-a6-test-britney-shuffle/* | sort | head -5
./spam_eval.sh cs489-2016w-qaovxtazypdl-a6-test-britney-shuffle

spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.TrainSpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.train.britney.txt --model cs489-2016w-qaovxtazypdl-a6-model-britney-shuffle --shuffle
spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.ApplySpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.test.qrels.txt --output cs489-2016w-qaovxtazypdl-a6-test-britney-shuffle --model cs489-2016w-qaovxtazypdl-a6-model-britney-shuffle
cat cs489-2016w-qaovxtazypdl-a6-model-britney-shuffle/* | head
cat cs489-2016w-qaovxtazypdl-a6-test-britney-shuffle/* | sort | head -5
./spam_eval.sh cs489-2016w-qaovxtazypdl-a6-test-britney-shuffle

spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.TrainSpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.train.britney.txt --model cs489-2016w-qaovxtazypdl-a6-model-britney-shuffle --shuffle
spark-submit --driver-memory 2g --class ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6.ApplySpamClassifier target/bigdata2016w-0.1.0-SNAPSHOT.jar --input data/spam/spam.test.qrels.txt --output cs489-2016w-qaovxtazypdl-a6-test-britney-shuffle --model cs489-2016w-qaovxtazypdl-a6-model-britney-shuffle
cat cs489-2016w-qaovxtazypdl-a6-model-britney-shuffle/* | head
cat cs489-2016w-qaovxtazypdl-a6-test-britney-shuffle/* | sort | head -5
./spam_eval.sh cs489-2016w-qaovxtazypdl-a6-test-britney-shuffle

