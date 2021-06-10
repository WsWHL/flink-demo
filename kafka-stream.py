from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import DataTypes, StreamTableEnvironment
from pyflink.table.descriptors import Json, Kafka, Rowtime, Schema

s_env = StreamExecutionEnvironment.get_execution_environment()
s_env.set_parallelism(1)
s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

st_env = StreamTableEnvironment.create(s_env)
st_env.get_config().get_configuration().set_string('pipeline.jars', 'file:///Users/eyri/Works/Example/flink-demo1/libs/flink-sql-connector-kafka_2.12-1.13.1.jar')
st_env.connect(Kafka()
    .version('universal')
    .topic('flink-test')
    .start_from_latest()
    .property('zookeeper.connect', 'localhost:2181')
    .property('bootstrap.servers', 'localhost:9092')
).with_format(Json().fail_on_missing_field(True).json_schema(
    "{"
    "  type: 'object',"
    "  properties: {"
    "    a: {"
    "      type: 'string'"
    "    },"
    "    b: {"
    "      type: 'string'"
    "    },"
    "    c: {"
    "      type: 'string'"
    "    },"
    "    time: {"
    "      type: 'string',"
    "      format: 'date-time'"
    "    }"
    "  }"
    "}"
)).with_schema(Schema()
    .field('rowtime', DataTypes.TIMESTAMP(3))
    .rowtime(Rowtime().timestamps_from_field('time').watermarks_periodic_bounded(60000))
    .field('a', DataTypes.STRING())
    .field('b', DataTypes.STRING())
    .field('c', DataTypes.STRING())
).create_temporary_table('source')

result = st_env.scan('source').select('a, b, c, rowtime').execute()
result.print()
