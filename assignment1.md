Question 1
=========================
Pairs Implementation:
-------------------------
Composed of two MapReduce jobs. The first job's mapper takes lines of text as input records, processes the first 100 words (made unique) into
records with key: word and value: 1. The combiner outputs records with key:word and value: number of times seen so far by a simple sum over values of same key.
The reducer then outputs records with key: word and value: the final count of the number of unique lines the word appears in (and in the first 100 words).

The second job takes lines of text as input records, pairs all words with each other within the line, and outputs
intermediate records for each pair with key: Pair(word1, word2), and value: 1. The combiner then outputs records with key: Pair(word1, word2) and
value: sum of iterated values over same key. The reducer first loads the side data from the first job, which are files of words and counts into a hashmap.
Using these counts, and a sum of the values of the same key, we can easily compute P(word1), P(word2) and P(word1 and word2) (<- this is the sum), and
output records with key: Pair(word1, word2) and value: PMI of the pair (float).

Stripes Implementation
-------------------------
Also composed of two MapReduce jobs. The first job's mapper takes lines of text as input records, processes the first 100 words (made unique) into
records with key: word and value: 1. The combiner outputs records with key:word and value: number of times seen so far by a simple sum over values of same key.
The reducer then outputs records with key: word and value: the final count of the number of unique lines the word appears in (and in the first 100 words).

The second job takes lines of text as input records, pairs all words with each other within the line, and outputs
intermediate records for each pair with key: word1, and value: Stripe of paired second words and their counts.
The combiner then outputs records with key: word1 and value: The stripes of paired second words and the sum of the iterated counts.
The reducer first loads the side data from the first job, which are files of words and counts into a hashmap.
Using these counts, for each second word in the stripe, we can easily compute P(word1), P(word2) and P(word1 and word2) (<- this is the stripe's value),
and output records with key: Pair(word1, word2) and value: PMI of the pair (float).

Question 2
=========================
Ran on my own laptop:
The running time of the complete pairs implementation was 33.346s
The running time of the complete stripes implementation was 33.174s

Question 3
=========================
Ran on my own laptop:
The running time of the complete pairs implementation was 34.131s
The running time of the complete stripes implementation was 44.548s

Question 4
=========================
15152 + 15480 + 15960 + 15288 + 15318 = 77198 PMI pairs.

Question 5
=========================
(maine, anjou)     3.6331422
Apparently, Maine and Anjou were two territories in france and one on Shakespeare's plays featured Margaret,
wife of Henry IV, in which she receives the lands of Maine and Anjou. I would imagine that the two are mentioned
within the same context many times, while being mentioned very rarely or none at all by itself.

Question 6
=========================
(tears, shed)      2.1117902
(tears, salt)      2.0528123
(tears, eyes)      1.165167
shed, salt, eyes have the highest PMI with tears.

(death, father's)  1.120252
(death, die)       0.75415933
(death, life)      0.7381346
father's, die, life have the highest PMI with death.

Question 7
=========================
(waterloo, kitchener)    2.6149974
(waterloo, napoleon)     1.9084398
(waterloo, napoleonic)   1.786619
kitchener, napoleon, napoleonic have the highest PMI with waterloo.

(toronto, marlboros)     2.3539965
(toronto, spadina)       2.3126037
(toronto, leafs)         2.3108904
marlboros, spadina, leafs have the highest PMI with toronto.


Q4p			1.5

Q4s			1.5

Q5p			1.5

Q5s			1.5

Q6.1p		1.5

Q6.1s		1.5

Q6.2p		1.5

Q6.2s		1.5

Q7.1p		1.5

Q7.1s		1.5

Q7.2p		1.5

Q7.2s		1.5

linux p		4

linux s		4

alti p		4

alti s		4

notes		

total		50

p stands for pair, s for stripe. linux p stands for run and compile pair in linux. 

If you have any question regarding to A1, plz come to DC3305 3~5pm on Friday (29th).
