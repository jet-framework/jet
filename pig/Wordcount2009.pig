SET default_parallel 40;

-- ########### script starts ############
register piggybank.jar;
register udfs.jar;

--import org.apache.pig.piggybank.evaluation.string;

--   val read = DList(getArgs(0))
--   val parsed = read.map(WikiArticle.parse(_, "\t"))
articles = LOAD '$input' USING PigStorage('\t') as (pageId : long, name : chararray, updated: chararray, xml: chararray, plaintext: chararray);
-- articles = LOAD '/home/stivo/master/testdata/wiki2009-articles-1k.tsv' USING PigStorage('\t') as (pageId : long, name : chararray, updated: chararray, xml: chararray, plaintext: chararray);


--    parsed
--      .map(_.plaintext)
plaintext = FOREACH articles GENERATE plaintext; -- CONCAT('\\n', plaintext);
plaintext_c1 = FOREACH plaintext GENERATE REPLACE($0, '\\[\\[.*?\\]\\]', ' ');
plaintext_c2 = FOREACH plaintext_c1 GENERATE REPLACE($0, '(\\\\[ntT]|\\.)\\s*(thumb|left|right)*', ' ');

--      .flatMap(_.split("[^a-zA-Z0-9']+").toSeq)
words = FOREACH plaintext_c2 GENERATE FLATTEN(dcdsl.udfs.CUSTOMSPLIT($0)) as word:chararray;
      
filtered_words_1 = FILTER words BY org.apache.pig.piggybank.evaluation.string.LENGTH(word) > 0;
filtered_words = FILTER filtered_words_1 BY NOT $0 matches '(thumb|left|right|\\d+px){2,}';
--      .map(x => (x, unit(1)))
--      .groupByKey
--      .reduce(_ + _)
word_groups = GROUP filtered_words BY word;
word_count = FOREACH word_groups GENERATE group AS word, COUNT(filtered_words) AS count;


--      .save(getArgs(1))
STORE word_count INTO '$output';





--plaintext_c1 = FOREACH plaintext GENERATE REPLACE($0, '\\[\\[.*?\\]\\]', ' ');

-- plaintext_c2 = FOREACH plaintext_c1 GENERATE REPLACE($0, '\\\\[nNt]', ' ');

--words = FOREACH plaintext_c1 GENERATE FLATTEN(TOKENIZE($0)) as word; 
-- words = FOREACH plaintext GENERATE FLATTEN(STRSPLIT(plaintext, ' ')) as word; --[^a-zA-Z0-9]+')) as word;

-- filtered_words = FILTER words BY org.apache.pig.piggybank.evaluation.string.LENGTH(word) > 1;

-- filter out any words that are just white spaces
--filtered_words = FILTER words BY word MATCHES '\\w+';
 
-- create a group for each word
-- word_groups = GROUP filtered_words BY word;
 
-- count the entries in each group
-- word_count = FOREACH word_groups GENERATE COUNT(filtered_words) AS count, group AS word;
 
--STORE ordered_word_count INTO '/tmp/number-of-words-on-internet';


