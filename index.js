const kafka = require("kafka-node");

//
// Class manages a kafka connection and produces kafka messages based on
// Volante events.
//
module.exports = {
  name: 'VolanteKafka',
  init() {
    if (this.configProps && this.enabled) {
      this.initialize();
    }
  },
  done() {
    if (this.producer) {
      this.producer.close();
    }
  },
  props: {
    enabled: true,
    host: '127.0.0.1',
    port: 9094,
  },
  data() {
    return {
      client: null,
      producer: null,
      publishedMessages: 0,
      refreshedTopics: [],
    };
  },
  events: {
    'VolanteKafka.publish'(topic, msg) {
      this.publish(...arguments);
    },
    'VolanteKafka.start'() {
      this.initialize();
    },
  },
  methods: {
    initialize() {
      try {
        let kafkaHost = `${this.host}:${this.port}`;
        this.$log(`setting up KafkaClient to ${kafkaHost}`);
        this.client = new kafka.KafkaClient({
          kafkaHost,
          connectTimeout: 5000,
          requestTimeout: 10000,
          autoConnect: true,
          connectRetryOptions: {
            forever: true,
          }
        });
        // handle client events
        this.client.on('close', (err) => {
          this.$error('KafkaClient error', err.error);
        });
        this.client.on('connect', () => {
          this.$log(`Connected to Kafka at ${kafkaHost}`);
        });

        // create producer
        this.producer = new kafka.Producer(this.client);
        // handle producer events
        this.producer.on('ready', () => {
          this.$log('ready to send kafka messages');
        });
        this.producer.on('error', (err) => {
          this.$error(err);
        });
      } catch (e) {
        this.$error('error initializing kafka-node', e);
      }
    },
    publish(topic, msg, callback) {
      // this.$isDebug && this.$debug('publish', topic, msg);
      this.client.refreshMetadata([topic], (err) => {
        if (err) {
          this.$error(err);
        }

        this.producer.send([{ topic, messages: [msg] }], (err, result) => {
          if (err) {
            this.$error(err);
            callback && callback(err);
            return;
          }
          this.$isDebug && this.$debug(`published message to ${topic}`);
          this.publishedMessages++;
          callback && callback(null, this.publishedMessages);
        });
      });
    },
  },
};
