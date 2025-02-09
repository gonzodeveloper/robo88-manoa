import pandas as pd
import logging
import socket
import struct
import yaml
import time
import json
import sys
import os


class SchedulerRequests:

    def __init__(self, config_file, debug=False):
        # Read config
        with open(config_file) as stream:
            self.config = yaml.safe_load(stream)

        # Connect to controller
        host = self.config["controller_ip"]
        port = self.config["controller_port"]
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))

        # Times out after 12 hours
        self.sock.settimeout(12 * 6000)

        # Get logger
        self.logger = logging.getLogger("robo_scheduler")
        # Log formatting
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger.setLevel(logging.DEBUG)
        # Log to file
        logfile = os.path.join(self.config['log_dir'], "robo_scheduler.log")
        handler = logging.FileHandler(logfile)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # Print to standard out if we are in debug mode
        if debug:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def add_request(self, request):
        """
        Load single observation into request database
        :param request: observation request dictionary, field definitions below:
                +-------------------+--------------+------+-----+---------+----------------+
                | Field             | Type         | Null | Key | Default | Extra          |
                +-------------------+--------------+------+-----+---------+----------------+
                | ra_hms            | varchar(16)  | NO   |     | NULL    |                |
                | dec_dms           | varchar(16)  | NO   |     | NULL    |                |
                | obj_name          | varchar(32)  | NO   |     | NULL    |                |
                | username          | varchar(32)  | NO   |     | NULL    |                |
                | priority          | int(11)      | NO   |     | NULL    |                |
                | window_start      | datetime     | NO   |     | NULL    |                |
                | window_finish     | datetime     | NO   |     | NULL    |                |
                | binning_mode      | varchar(16)  | NO   |     | NULL    |                |
                | filters           | varchar(32)  | NO   |     | NULL    |                |
                | exp_times         | varchar(32)  | NO   |     | NULL    |                |
                | exp_num           | int(11)      | NO   |     | NULL    |                |
                | dither            | varchar(32)  | NO   |     | NULL    |                |
                | stack             | varchar(32)  | NO   |     | NULL    |                |
                | cadence           | varchar(32)  | NO   |     | NULL    |                |
                | ns_ref_time       | varchar(32)  | YES  |     | NULL    |                |
                | ns_rate_ra        | float        | YES  |     | NULL    |                |
                | ns_rate_dec       | float        | YES  |     | NULL    |                |
                | max_airmass       | float        | YES  |     | NULL    |                |
                | min_moon_sep      | float        | YES  |     | NULL    |                |
                | follow_on_id      | int(11)      | YES  |     | NULL    |                |
                | follow_on_lag_min | int(11)      | YES  |     | NULL    |                |
                | follow_on_lag_max | int(11)      | YES  |     | NULL    |                |
                | too               | varchar(16)  | YES  |     | NULL    |                |
                | comments          | varchar(256) | YES  |     | NULL    |                |
                +-------------------+--------------+------+-----+---------+----------------+
        :return: request index of target inserted.
        """
        # Remove just in case
        request['ra_hms'] = request['ra_hms'].replace("ra=", "")
        request['dec_dms'] = request['dec_dms'].replace("dec=", "")
        # Send request to change night (don't send nulls in JSON)
        status, msg = self._send_request(f"add_req {json.dumps(request)}")

        if status == "ERROR":
            self.logger.error(msg)
            raise RuntimeError(msg)

        # Return target idx
        return msg

    def get_requests(self, username):
        """
        Get requests submitted by the given user.
        :param username: name of user associated with requests; str.
        :return: pd.Dataframe
        """
        # Send request for obs_requests
        status, msg = self._send_request(f"get_reqs {username}")

        if status == "ERROR":
            self.logger.error(msg)
            raise RuntimeError(msg)

        msg_dict = json.loads(json.loads(msg))
        # Read dataframe from JSON
        return pd.json_normalize(msg_dict)

    def get_recent_observation(self, username, n_nights=None, obj_name=None):
        """
        Retrieve observations for a given user within search spec
        :param username: name of the user who submitted obs request; str.
        :param n_nights: how many nights back to search, if set; str.
        :param obj_name: name of object to search, if set; str.
        :return: list of files
        """
        # Send request to change night (don't send nulls in JSON)
        args = locals()
        request = {k: v for k, v in args.items() if v is not None and k != "self"}
        status, msg = self._send_request(f"get_obs {json.dumps(request)}")

        if status == "ERROR":
            self.logger.error(msg)
            raise RuntimeError(msg)

        # Return list of files (sub out /obs/data/ for local data_dir)
        msg.replace("/obs/data/", self.config["data_dir"])
        return msg.split()

    def remove_request(self, username, obj_name=None, request_idx=None):
        """
        Remove one or multiple observation requests from database.
        :param username: name of user who submitted obs request; str.
        :param obj_name: remove all requests with given object name; str
        :param request_idx: remove single request with given index; int
        :return: number of requests removed
        """
        # Send request to change night (don't send nulls in JSON)
        args = locals()
        request = {k: v for k, v in args.items() if v is not None and k != "self"}
        status, msg = self._send_request(f"del_req {json.dumps(request)}")

        if status == "ERROR":
            self.logger.error(msg)
            raise RuntimeError(msg)

        # Return number of lines removed
        return msg

    def _send_request(self, request):
        # Send bytes
        send_msg(sock=self.sock, msg=bytes(request, "ascii"))
        # Debug log
        self.logger.debug({'status': 'sending_message', 'request': request})

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
        self.logger.debug({'status': 'recieved_response', 'request': message})

        return status, response


########################################################################
# Utility functions for communicating with Robo88 Scheduler
########################################################################
def recvall(sock, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data


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
