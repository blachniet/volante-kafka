module.exports = {
  name: 'KafkaExample',
  events: {
    'VolanteKafka.ready'() {
      this.test();
    },
  },
  methods: {
    async test() {
    }
  }
};