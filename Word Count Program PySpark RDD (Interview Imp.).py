# Databricks notebook source
# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

dbutils.fs.put("/FileSore/tables/rrr.txt", """RRR[note 1] (subtitled onscreen as Roudram Ranam Rudhiram) is a 2022 Indian Telugu-language epic period action drama film directed by S. S. Rajamouli, who co-wrote the film with V. Vijayendra Prasad. It was produced by D. V. V. Danayya of DVV Entertainment. The film stars N. T. Rama Rao Jr., Ram Charan, Ajay Devgn, Alia Bhatt, Shriya Saran, Samuthirakani, Ray Stevenson, Alison Doody, and Olivia Morris. It revolves around the fictional versions of two Indian revolutionaries, Alluri Sitarama Raju (Charan) and Komaram Bheem (Rama Rao), their friendship, and their fight against the British Raj.

Rajamouli came across stories about the lives of Rama Raju and Bheem and connected the coincidences between them, imagining what would have happened had they met, and been friends. The film was announced in March 2018. Principal photography of the film began in November 2018 in Hyderabad and continued until August 2021, owing to delays caused by the COVID-19 pandemic. It was filmed extensively across India, with a few sequences filmed in Ukraine and Bulgaria. The film's soundtrack and background score were composed by M. M. Keeravani, with cinematography by K. K. Senthil Kumar and editing by A. Sreekar Prasad. Sabu Cyril is the film's production designer whilst V. Srinivas Mohan supervised the visual effects.

Made on a budget of ₹550 crore (US$69 million),[8] RRR is one of the most expensive Indian films to date. The film was initially scheduled for theatrical release on 30 July 2020, which was postponed multiple times due to production delays and the COVID pandemic. RRR was released theatrically on 25 March 2022. With ₹240 crore (US$30 million) worldwide on its first day, RRR recorded the highest opening-day earned by an Indian film. It emerged as the highest-grossing film in its home market of Andhra Pradesh and Telangana, grossing over ₹415 crore (US$52 million).[9] The film grossed ₹1,387.26 crore (US$170 million) worldwide, setting several box office records for an Indian film, including the third highest-grossing Indian film, second highest-grossing Telugu film, highest grossing Telugu film of 2022 and highest grossing Indian film of 2022 worldwide.[5][10]

RRR received universal praise for its direction, screenwriting, cast performances, cinematography, soundtrack, action sequences and VFX. The film was considered one of the ten best films of the year by the National Board of Review, making it only the seventh non-English language film ever to make it to the list.[citation needed] The song "Naatu Naatu" won the Oscar for Best Original Song at the 95th Academy Awards, making it the first song from an Indian film, as well as the first from an Asian film, to win in this category. This made the film[11][12] the first Indian film by an Indian production to win an Academy Award. The film became the third Indian film and first Telugu film to receive nominations at the Golden Globe Awards, including Best Foreign Language Film, and won Best Original Song for "Naatu Naatu", making it the first Indian (as well as the first Asian) nominee to win the award.[13][14] RRR also won the awards for Best Foreign Language Film and Best Song at the 28th Critics' Choice Awards. At the 69th National Film Awards, the film won 6 awards, including Best Popular Feature Film, Best Music Direction (Keeravani) and Best Male Playback Singer (Kaala Bhairava for "Komuram Bheemudo").

Plot
During the British Raj in 1920, Governor Scott Buxton and his wife Catherine visit a forest in Adilabad, where they abduct Malli, an artistically talented young girl from the Gond tribe. Enraged, the tribe's guardian Komaram Bheem embarks for Delhi to rescue her, disguising himself as a Muslim named Akhtar. The Nizamate of Hyderabad gets sympathetic to the Raj and warns Scott's office of the impending danger. Undeterred, Catherine enlists A. Rama Raju, an ambitious Indian Imperial Police officer to quell the threat. Raju and his uncle Venkateswarulu attend several pro-independence gatherings where he feigns to support independence.

Bheem's gullible aide Lachhu believes Raju's ruse, where he attempts to recruit him into Bheem's plot. Before taking Raju to Bheem, Lachhu discovers Raju's identity and flees. Shortly after, Bheem and Raju encounter each other, unaware of their opposing allegiances. While working together to save a boy from a train wreck, they form a friendship, gradually growing close. Raju assists Bheem in courting Scott's niece Jenny, unaware that Bheem's agenda is to infiltrate Scott's residence. When Jenny takes Bheem to her residence, Bheem locates the room where Malli is held captive and promises to free her. Raju locates Lachhu and apprehends him. While being interrogated, Lachhu captures and sets a banded krait onto Raju; it attacks him; he then warns him of his imminent death and that the antidote is only known to the Gonds.

Dazed, Raju approaches Bheem, who immediately tends to him. Noticing similar religious features between Lachhu and Bheem, Raju deduces his true intentions. However, Bheem himself divulges his tribal identity and his mission, unaware of Raju's true identity. At an event to honour Scott, Bheem's men barge into his residence with a lorry filled with wild animals, creating havoc among the guests. The animals maul Scott's guards, allowing Bheem to briefly gain the upper hand; however, Raju arrives and they fight briefly but Bheem is forced to give up when Scott threatens to kill Malli. Raju is promoted for thwarting Bheem, yet he is guilt-stricken over his actions, recalling his pro-nationalistic background and his alter-ego as a mole within the police; he sought a promotion to gain access to gun shipments to smuggle to his village to fulfill a promise made to his dying revolutionary father Alluri Venkatarama Raju.

At Bheem's public flogging, Raju attempts to persuade him to recant his actions; Bheem chooses flogging instead. Bheem sings in defiance of his injuries, inciting the assembled crowd into rebellion. The riot further enlightens Raju, who finally recognises his reckless actions. Determined to save Bheem, Raju persuades Scott to execute Bheem in secrecy, while preparing an ambush to rescue him. However, Scott figures out the scheme. While rescuing Malli from Scott's men, Raju is grievously injured. Bheem, who freed himself, mistakenly interprets Raju's actions as an attempt to kill Malli; he bludgeons him and escapes with her, leading to Raju's capture. Months later, Bheem, who is hiding with his group and Malli in Hathras, is cornered by the colonial authorities. He narrowly avoids being exposed when Raju's fiancée Sita, repels them by claiming there is a smallpox epidemic.

Unaware of Bheem's identity, she reveals Raju's anti-colonial objectives and his impending execution. Crestfallen upon realising his folly, Bheem vows to save him. With Jenny's sympathetic assistance, Bheem infiltrates the barracks where Raju is detained and frees him. The pair retreat to a nearby forest, where they decimate soldiers with a longbow taken from a Rama shrine and a spear. Taking the fight to Scott, they hurl a flaming motorcycle into the barracks' magazines, setting it afire. Bheem steals a cache of guns for Raju before the barracks are destroyed. The subsequent explosion kills many soldiers and Catherine. Cornering a wounded Scott, Raju has Bheem execute him with a British rifle, fulfilling their respective objectives. Ram is reunited with Sita, and Bheem is reunited with Jenny and his family. To mark the mission's successful completion, Raju asks Bheem to make a wish he can grant; Bheem asks Raju to provide education for him and his community.""")

# COMMAND ----------

# DBTITLE 1,Command to read file from File System
# MAGIC %fs head /FileSore/tables/rrr.txt

# COMMAND ----------

rrr_rdd = sc.textFile("/FileSore/tables/rrr.txt")

# COMMAND ----------

rrr_rdd.getNumPartitions()

# COMMAND ----------

rrr_rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Diff. between Map and FlatMap
# MAGIC
# MAGIC Map ---> It will provide results in nested collection.
# MAGIC
# MAGIC flatMap ---> This will provide results in general way.

# COMMAND ----------

rdd_words = rrr_rdd.flatMap(lambda x:x.lower().split(" "))
rdd_valid = rdd_words.filter(lambda a:a!='')
rdd_kv = rdd_valid.map(lambda a:(a,1))
rdd_count = rdd_kv.reduceByKey(lambda a,b:a+b)
#rdd_count.sortByKey().collect()    
#Default will be Ascending ('(charan)', 1) ---> Charan is Key and 1 is Value. Sort based on Key)
rdd_final = rdd_count.map(lambda a: (a[1], a[0])).sortByKey(False)
#sortByKey(False) ---> Descending
rdd_final.coalesce(1).saveAsTextFile("/rdd_word_count/")

# COMMAND ----------

# MAGIC %fs ls /rdd_word_count/

# COMMAND ----------

# MAGIC %fs head dbfs:/rdd_word_count/part-00000

# COMMAND ----------

rdd_final.toDF(["count","word"]).show()