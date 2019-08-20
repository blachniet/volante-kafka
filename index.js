const Kafka = require('node-rdkafka');

//
// Class manages a kafka connection and produces kafka messages based on
// Volante events.
//
module.exports = {
	name: 'VolanteKafka',
	events: {
	  'VolanteKafka.message'(msg) {
      this.sendMessage(msg);
	  }
  },
  init() {
    this.$log('librdkafka version', Kafka.librdkafkaVersion);
    this.$log('librdkafka features', Kafka.features);
  },
  done() {
	  if (this.stream) {
	    this.stream.close();
	  }
  },
	props: {
	  topic: '',
    kafkaOptions: {},
  },
  data: {
    stream: null,
  },
	updated() {
	  if (this.stream) {
	    this.stream.close();
	  }
	  this.initializeStream();
	},
	methods: {
	  initializeStream() {
      try {
	      this.stream = new Kafka.Producer.createWriteStream(this.kafkaOptions, {}, {
	        topic: this.topic,
	      });
      } catch (e) {
	  		this.$error('error initializing kafka stream', e);
	  	}
	  },
	  sendMessage(msg) {
	  	try {
		    let queuedSuccess = this.stream.write(Buffer.from(msg));
		    if (!queuedSuccess) {
		      this.$warn('did not queue message:', msg);
		    }
	  	} catch (e) {
	  		this.$error('error sending kafka msg', e);
	  	}
	  }
	},
};
