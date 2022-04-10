from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace, lower, expr, split, filter, explode, size, udf, create_map, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType, IntegerType, MapType
from itertools import chain
from collections import Counter

TWITTER_LINEBREAK_SPACE_REGEX = r'\\n|\s'
TWITTER_SPECIAL_REMOVE_REGEX = r'(@\w+)|(#)|(https\S+)'
EMOJI_REMOVE_REGEX = r'[^\x00-\xff]+'
PUNCTUATION_REMOVE_REGEX = r'[\x21-\x2f]|[\x3a-\x40]|[\x5b-\x60]|[\x7b-\xbf]'

TEAMS_SYNONYMS_MAPPING = {
    'fortaleza': 'fortaleza',
    'ceará': 'ceara',
    'américa mg': "america_mg",
    'américamg': "america_mg",
    'américa mineiro': "america_mg",
    'athletico': "athletico",
    'atlético go': "atletico_go",
    'atléticogo': "atletico_go",
    'atlético goianiense': "atletico_go",
    'atlético mg': "atletico_mg",
    'atléticomg': "atletico_mg",
    'atlético mineiro': "atletico_mg",
    'avai': "avai",
    'botafogo': "botafogo",
    'bragantino': "bragantino",
    'corinthians': "corinthians",
    'coritiba': "coritiba",
    'cuiaba': "cuiaba",
    'flamengo': "flamengo",
    'fluminense': "fluminense",
    'goias': "goias",
    'internacional': "internacional",
    'juventude': "juventude",
    'palmeiras': "palmeiras",
    'santos': "santos",
    'são paulo': "sao_paulo"
}

CONTEXTS = [
    'futebol',
    'escalação',
    'placar',
    'partida',
    'confronto',
    'enfrenta',
    'jogador',
    'jogo',
    'jogar',
    'time',
    'equipe',
    'seleção',
    'contrato',
    'contratação',
    'plantel',
    'ganhar',
    'ganhou',
    'vence',
    'venceu',
    'ganha',
    'ganhou',
    'bola',
    'chuteira',
    'estádio',
    'ingresso',
    'gramado',
    'trave',
    'travessão',
    'chute',
    'defesa',
    'ataque',
    'atacante',
    'zagueiro',
    'zaga',
    'lateral',
    'falta',
    'expulsão',
    'cartão',
    'perde',
    'perdeu',
    'rebaixamento',
    'vitória',
    'derrota',
    'brasileirão',
    'campeonato',
    'copa',
    'lance',
    'gol',
    'rodada',
    'enfrenta',
    'campeão',
    'rival',
    'rivais',
    'título',
    'campeões',
    'uniforme',
    'camisa',
    'libertadores',
    'transmissão',
    'assistir',
    'patrocinador',
    'patrocínio',
    'torcida',
    'torcedor',
    'arquibancada',
    'esporte',
    'juiz',
    'var',
    'arbitragem',
    'série a'
]

teamsRegex = "|".join(list(TEAMS_SYNONYMS_MAPPING.keys()))
contextRegex = "|".join(CONTEXTS)


spark = SparkSession \
    .builder \
    .appName("TweetsProcessing") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Carregar o schema dos tweets com StructType
schema = StructType([
    StructField("created_at", TimestampType(), False),
    StructField("id", DecimalType(19), False),
    StructField("text", StringType(), False)
])

# readStream é um atributo de SparkSession utilizado para ler streams, recebendo format e options
# Existe também o atributo read, para leituras em batch
# Método load carrega dados de uma fonte retornando-os como um Dataframe
lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweets") \
    .load()

# Possível também utilizar o select + cast
df_str = lines.select(col("value").cast("string"))

# Parseando json a partir de uma string
df = df_str.select(
    from_json(col("value"), schema).alias("json")
).select("json.*")


# Pré-tratamento do texto
df = df.withColumn('text', regexp_replace(col('text'), TWITTER_SPECIAL_REMOVE_REGEX, ''))
df = df.withColumn('text', regexp_replace(col('text'), EMOJI_REMOVE_REGEX, ''))
df = df.withColumn('text', regexp_replace(col('text'), PUNCTUATION_REMOVE_REGEX, ''))
df = df.withColumn('text', lower(col('text')))

# Identificar times
df = df.withColumn('teams', expr(f"array_distinct(regexp_extract_all(text, '{teamsRegex}', 0))"))

# Identificar contextos
df = df.withColumn('contexts', expr(f"array_distinct(regexp_extract_all(text, '{contextRegex}', 0))"))

# Filtrando
df = df.where(size(col('teams')) > 0)
df = df.where(size(col('contexts')) > 0)

# Contagem das palavras
df = df.withColumn("words", split(col('text'), TWITTER_LINEBREAK_SPACE_REGEX, -1))
df = df.withColumn("words", filter(col('words'), lambda x: x != ''))

udf_counter = udf(
    lambda x: dict(Counter(x)),
    MapType(StringType(), IntegerType())
)
df = df.withColumn("words_count", udf_counter(col("words")))

# Explode and map teams
mapping_expr = create_map([lit(x) for x in chain(*TEAMS_SYNONYMS_MAPPING.items())])
df = df.withColumn("team", explode(col("teams")))
df = df.withColumn("team", mapping_expr[col("team")])

# Escrevendo a stream em um tópico Kafka
# Similar ao readStream/read, existem os atributos writeStream/write, 
#   que culminam no método start() ou save(), respectivamente
# Para escrever no Kafka, é preciso determinar um checkpointLocation
# Estou utilizando o selectExpr para encapsular o df em um json "value", 
#   para enviar os dados ao Kafka novamente
df = df.select('created_at', 'id', 'words_count', 'contexts', 'team')
query = df \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "tweets_treated") \
    .option("checkpointLocation", "checkpoint/directory") \
    .start()
query.awaitTermination()
