from logstash_async.handler import AsynchronousLogstashHandler
from confluent_kafka import Producer
from threading import Thread
import multiprocessing as mp
import json
import warnings
import subprocess
import numpy as np
import time
import redis
import traceback
import yaml
import socket
import struct
import logging
import os
import re
import sys

# Reporting mode flags
PRINT = 1
LOGFILE = 2
DATABASE = 4
STATE_MACHINE = 8
KAFKA = 16
FULL = 31

# Report levels
BUFFER_SIZE = 2048


# Messaging protocol
def send_msg(sock, msg):
    # Prefix each message with a 4-byte length (network byte order)
    msg = struct.pack('>I', len(msg)) + msg
    sock.sendall(msg)


def recv_msg(sock):
    # Read message length and unpack it into an integer
    raw_msglen = recvall(sock, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    # Read the message data
    return recvall(sock, msglen)


def recvall(sock, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data


def parse_network_str(netstr):
    """
    Parses command strings exchanged over the network.
    :param netstr: command string from socket; str
    :return: tuple with command/status and following message; (str, str)
    """
    command_status = netstr.split()[0].strip()
    msg = " ".join(netstr.split()[1:])
    return command_status, msg

class Robot:
    """
    A class for managing logging and state machine for the Robo88 modules
    """

    def __init__(self, module, reporting_mode, config_dir, debug=False):
        """
        Creates a Robot object that handles the logging and state machine for the program.

        :param module: name of the module for this bot; string
        :param reporting_mode: flag indicating reporting mode; e.g. PRINT, STATE_MACHINE, PRINT | STATE_MACHINE, FULL
        :param config_file: YAML configuration file path; string
        :param debug: logging mode; boolean
        """
        # Read config
        self.config_dir = config_dir
        config_file = os.path.join(config_dir, module + ".conf")
        with open(config_file) as stream:
            self.config = yaml.safe_load(stream)

        # Set module name
        self.module = module

        # Get logger
        self.logger = logging.getLogger(module)

        # Set log level
        if debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        self.debug = debug
        # Log formatting
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Set reporting mode
        self.reporting_mode = reporting_mode

        # Print to system standard out
        if PRINT & reporting_mode:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # Log to file
        if LOGFILE & reporting_mode:
            logfile = os.path.join(self.config['log_dir'], self.module + ".log")
            handler = logging.FileHandler(logfile)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # Submit to logstash
        if DATABASE & reporting_mode:
            handler = AsynchronousLogstashHandler(host=self.config['logstash_host'],
                                                  port=self.config['logstash_port'],
                                                  certfile=self.config['logstash_cert'],
                                                  database_path=None)
            self.logger.addHandler(handler)

        # Redis connection pool for state machine
        if STATE_MACHINE & reporting_mode:
            self.state_machine = redis.ConnectionPool(host=self.config['redis_host'],
                                                      port=6379,
                                                      db=0,
                                                      password=self.config['redis_pass'])
        # Otherwise set dummy value
        else:
            self.state_machine = None

        # Kafka connection for transport messages
        if KAFKA & reporting_mode:
            # Kafka Prooducer
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=DeprecationWarning)
                conf = {'bootstrap.servers': self.config['kafka_address'],
                        #'delivery.timeout.ms': 1000,
                        'queue.buffering.max.messages': 1000,
                        'queue.buffering.max.ms': 5000,
                        'batch.num.messages': 16,
                        'client.id': socket.gethostname()}
                self.producer = Producer(conf)
        else:
            self.producer = None

    def get_state(self):
        """
        Gives the full state machine status for the module
        :return: dictionary
        """
        # Check dummy value
        if self.state_machine is None:
            return None

        # Get Redis connection
        conn = redis.Redis(connection_pool=self.state_machine)

        # Set Redis module for stacam
        module = "stacam_archon_controller" if self.module == "stacam" else self.module

        # Get state of module and tcs
        module_status = conn.hgetall(module)
        tcs_status = conn.hgetall("tcs")

        # Combine
        return self._parse_redis(module_status, self.module), self._parse_redis(tcs_status, "tcs")

    def get_state_item(self, field, module=None):
        """
        Get single state field for the module.
        """
        # Get Redis connection
        conn = redis.Redis(connection_pool=self.state_machine)
        # Set Redis module for stacam
        module = "stacam_archon_controller" if module == "stacam" else module

        module_set = module if module is not None else self.module
        res = conn.hget(module_set, field)
        return None if res is None else res.decode()

    def update_state(self, status):
        """
        Updates the Robot object's state machine
        :param status: dictionary with status field-value pairs
        :return: the number of fields that were added; int
        """
        # Check dummy value
        if self.state_machine is None:
            return None

        # Get Redis connection
        conn = redis.Redis(connection_pool=self.state_machine)

        # Update
        return len([conn.hset(self.module, k, v) for k, v in status.items()])

    def log(self, message, level):
        """
        Reports the modules status to the state machine and or logs
        :param message: dictionary with status field-value pairs (note that booleans must be submitted as 1s or 0s);
                        i.e. {"POWERON" 1, "SETUP_STATUS": "Connecting to TCS"}
        :param level: the priority level of this message; "INFO" or "DEBUG";
                      if the module setting is set to debug=False, then "DEBUG" level messages will be ignored
        :return:  the number of fields logged or updated to state machine
        """
        # Do not log if we only have a state machine
        if self.reporting_mode == STATE_MACHINE:
            return 0

        # Mappings
        log_func = {"INFO": self.logger.info, "DEBUG": self.logger.debug, "ERROR": self.logger.error}
        # Call correct func
        log_func[level](message, extra=message)

        return len(message.keys())

    def acked(self, err, msg):
        if err is not None:
            self.log({'kafka_error': str(err)}, level="ERROR")

    def publish(self, message):
        # Skip if we have no producer
        if self.producer is not None:
            # Submit for phot processing in local Kafka queue
            self.producer.produce(topic='robo_img_queue', value=json.dumps(message).encode('utf-8'),
                                  callback=self.acked)
            self.producer.poll(timeout=1.0)

    def _parse_redis(self, redis_status, module):
        """
        Internal method for parsing redis byte values into correct data types
        :param redis_status: raw redis return value
        :return: dictionary
        """
        cast = {
            "float": np.float32,
            "str": str,
            "int": np.uint16
        }
        status = {}
        for k, v in redis_status.items():
            key = k.decode()
            val = v.decode()
            # Cast into correct data type if specified
            if "fits_headers" in self.config.keys() and key in self.config['fits_headers'][module].keys():
                status[key] = cast[self.config['fits_headers'][module][key]['dtype']](val)
            # Otherwise cast as string
            else:
                status[key] = val

        return status

    def exec_shell(self, cmd_str):
        """
        Wrapper function to run outside commands
        :param cmd_str: shell command; str
        :return: stdout; str
        """
        # Reduce str
        cmd_str = re.sub(' +', ' ', cmd_str)
        # Log
        self.log({"status": cmd_str}, level="DEBUG")
        # Run external command
        result = subprocess.run(cmd_str, stdout=subprocess.PIPE, shell=True)
        return result.stdout.decode('utf-8')


class ControllerBot(Robot):

    def __init__(self, *args, **kwargs):

        # Load the Robot params
        super().__init__(*args, **kwargs)

        # Setup listening socket
        self.sock = socket.socket()
        host = self.config["listener_ip"]
        port = self.config["listener_port"]
        self.sock.bind((host, port))

        # Define mulit-threaded (forked) or no
        self.forked = self.config['forked']

        # Send report
        self.log({'setup_status': f"socket binded to {host}:{port}"}, level='INFO')

    def run(self):
        # Let socket listen
        self.sock.listen(5)
        self.log({'setup_status': f"socket listening"}, level='INFO')

        # Keep a list of child processes for termination
        children = []
        while True:
            # Get new client connection
            conn, addr = self.sock.accept()

            # Times out after 12 hours
            conn.settimeout(12 * 6000)

            if self.forked:
                # Start client thread
                p = mp.Process(target=self.client_thread, args=(conn, addr, ))
                p.start()
                children.append(p)

                # Clean any finished children
                new_children = []
                for p in children:
                    p.join(0.0)
                    if p.is_alive():
                        new_children.append(p)
                    else:
                        p.close()
                # Keep any client processes still running
                children = new_children
            else:
                # Start client thread
                Thread(target=self.client_thread, args=(conn, addr,)).start()

    def client_thread(self, conn, addr):
        msg_str = ""
        while True:
            # Receive command
            msg = recv_msg(conn)
            if msg is None:
                continue

            request = msg.decode('ascii')
            if request == '':
                continue
            command = None
            # Try command
            try:
                command, msg_str = parse_network_str(request)

                # Break out if client closes
                if command == "close":
                    send_msg(sock=conn, msg=bytes("done closed", 'ascii'))
                    break

                # Otherwise run controller logic
                response = self.controller_logic(command, msg_str)
                status = "done"

            except Exception as e:
                print(traceback.format_exception(e))
                response = str(e)
                status = "ERROR"

            # Respond (always null terminate to match tcs stuff)
            send_msg(sock=conn, msg=bytes(f"{status} {response}", "ascii"))

            # Report and remove
            message = dict({
                "command": command,
                "client_message": msg_str,
                "status": status,
                "response": response,
                "client": str(addr)
            })
            self.log(message, level="INFO")



    def controller_logic(self, command, msg):
        raise NotImplementedError("controller logic must be implemented by subclass")


class DispatchBot(Robot):

    def __init__(self, *args, **kwargs):
        # Load the Robot params
        super().__init__(*args, **kwargs)

        # Connect to controller
        host = self.config["controller_ip"]
        port = self.config["controller_port"]
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))

        # Times out after 12 hours
        self.sock.settimeout(12 * 6000)

        # Report
        message = {"setup_status": "connected to controller"}
        self.log(message, level="DEBUG")

    def send_request(self, request):
        # Send bytes
        send_msg(sock=self.sock, msg=bytes(request, "ascii"))
        # Debug log
        self.log({'status': 'sending_message', 'request': request}, level="DEBUG")

        # Wait a sec and recieve response
        time.sleep(1)
        msg = recv_msg(sock=self.sock).decode('ascii')

        status = msg.split()[0]
        response = ' '.join(msg.split()[1:])

        # Report and remove
        message = dict({
            "request": request,
            "status": status,
            "response": response
        })
        self.log(message, level="DEBUG")

        return status, response

    def close(self):
        self.send_request("close")
