import asyncio

from kinopio_hub_ros.errors import ServiceCallError


class FakeServiceClient:
    def __init__(self, service_name, service_type):
        self.service_name = service_name
        self.service_type = service_type


class FakeRos2Driver:
    def __init__(self, *, distro="humble", topics=(), services=None, service_ready=None):
        self.distro = distro
        self._topics = list(topics)
        self._services = dict(services or {})
        self._service_ready = dict(service_ready or {})
        self.started_with = None
        self.shutdown_called = False
        self.subscriptions = {}
        self.subscription_types = {}
        self.publishers = {}
        self.publisher_types = {}
        self.published = []
        self.spin_calls = []
        self.service_clients = {}
        self.service_calls = []
        self.removed_service_futures = []

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

    def load_service_type(self, service_type):
        return service_type

    def create_service_client(self, service_name, service_type):
        client = FakeServiceClient(service_name, service_type)
        self.service_clients[(service_name, service_type)] = client
        return client

    def service_client_ready(self, client):
        return self._service_ready.get(client.service_name, True)

    def call_service_async(self, client, data):
        self.service_calls.append((client.service_name, client.service_type, data))
        future = asyncio.get_running_loop().create_future()
        response = self._services.get(client.service_name, {})
        if response == "__pending__":
            return future
        if isinstance(response, Exception):
            future.set_exception(response)
        else:
            future.set_result(response)
        return future

    def service_call_done(self, future):
        return future.done()

    def service_call_result(self, client, future):
        return future.result()

    def remove_pending_service_request(self, client, future):
        self.removed_service_futures.append((client.service_name, future))

    def call_service(self, service_name, service_type, data, timeout_sec=None):
        self.service_calls.append((service_name, service_type, data))
        if not self._service_ready.get(service_name, True):
            raise ServiceCallError(
                "ROS service is not available: {0}".format(service_name),
                code="service_unavailable",
            )
        response = self._services.get(service_name, {})
        if isinstance(response, Exception):
            raise response
        return response

    def spin_once(self, *, timeout_sec):
        self.spin_calls.append(timeout_sec)

    def emit(self, topic, text, json_value=None):
        self.subscriptions[topic](text, json_value)


class FakeRos1Driver(FakeRos2Driver):
    pass
