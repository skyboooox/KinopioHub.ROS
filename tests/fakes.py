class FakeRos2Driver:
    def __init__(self, *, distro="humble", topics=()):
        self.distro = distro
        self._topics = list(topics)
        self.started_with = None
        self.shutdown_called = False
        self.subscriptions = {}
        self.subscription_types = {}
        self.publishers = {}
        self.publisher_types = {}
        self.published = []
        self.spin_calls = []

    def start(self, *, node_name, qos_config):
        self.started_with = {
            "node_name": node_name,
            "qos_config": qos_config,
        }

    def shutdown(self):
        self.shutdown_called = True

    def list_topics_and_types(self):
        return tuple((topic, tuple(types)) for topic, types in self._topics)

    def create_text_subscription(self, topic, message_type, callback):
        self.subscriptions[topic] = callback
        self.subscription_types[topic] = message_type
        return topic

    def create_text_publisher(self, topic, message_type):
        self.publishers[topic] = topic
        self.publisher_types[topic] = message_type
        return topic

    def publish_text(self, publisher, text):
        self.published.append((publisher, text))

    def load_message_type(self, message_type):
        return message_type

    def spin_once(self, *, timeout_sec):
        self.spin_calls.append(timeout_sec)

    def emit(self, topic, text, json_value=None):
        self.subscriptions[topic](text, json_value)


class FakeRos1Driver(FakeRos2Driver):
    pass
