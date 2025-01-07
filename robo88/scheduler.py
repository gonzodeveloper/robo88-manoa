from .sys_mods import DispatchBot
import pandas as pd
import json


class SchedulerRequests(DispatchBot):

    def __init__(self, *args, **kwargs):
        # Load the Robot params
        super().__init__("scheduler_requests", *args, **kwargs)

    def get_recent_observation(self, user, n_nights=None, obj_name=None):
        """
        Retrieve observations for a given user within search spec
        :param user: name of the user who submitted obs request; str.
        :param n_nights: how many nights back to search, if set; str.
        :param obj_name: name of object to search, if set; str.
        :return: list of files
        """
        # Send request to change night (don't send nulls in JSON)
        args = locals()
        request = {k: v for k, v in args.items() if v is not None}
        status, msg = self.send_request(f"get_obs {json.dumps(request)}")

        if status == "ERROR":
            self.log({"runtime_error": msg}, level="ERROR")
            raise RuntimeError(msg)

        # Return list of files (sub out /obs/data/ for local data_dir)
        msg.replace("/obs/data/", self.config["data_dir"])
        return msg.split()

    def add_request(self, obj_name, ra_hms, dec_dms, user, priority,
                    camera_mode=None, filter_name=None, exp_time=None, exp_num=None, comments=None):
        """
        Load single observation into request database
        :param obj_name: standard object name; str.
        :param ra_hms: right ascension; HH:MM:SS.S; str.
        :param dec_dms: declination; DD:MM:SS.S; str.
        :param user: name of requesting user (no spaces); str.
        :param priority: ranked priority of observation 1-5 for survey priorities; 9 for immediate; int.
        :param camera_mode: binning mode for camera, e.g. BIN2 or BIN5
        :param filter_name: filter to use for target (Open, g, r, i, z, y)
        :param exp_time: exposure time in seconds; int.
        :param exp_num: number of dithered exposures to take; int.
        :param comments: general comments; str
        :return: request index of target inserted.
        """
        # Send request to change night (don't send nulls in JSON)
        args = locals()
        request = {k: v for k, v in args.items() if v is not None}
        status, msg = self.send_request(f"add_req {json.dumps(request)}")

        if status == "ERROR":
            self.log({"runtime_error": msg}, level="ERROR")
            raise RuntimeError(msg)

        # Return target idx
        return msg

    def get_requests(self, user):
        """
        Get requests submitted by the given user.
        :param user: name of user associated with requests; str.
        :return: pd.Dataframe
        """
        # Send request for obs_requests
        status, msg = self.send_request(f"get_reqs {user}")

        if status == "ERROR":
            self.log({"runtime_error": msg}, level="ERROR")
            raise RuntimeError(msg)

        # Read dataframe from JSON
        return pd.DataFrame(json.loads(msg))

    def remove_request(self, user, obj_name=None, request_idx=None):
        """
        Remove one or multiple observation requests from database.
        :param user: name of user who submitted obs request; str.
        :param obj_name: remove all requests with given object name; str
        :param request_idx: remove single request with given index; int
        :return: number of requests removed
        """
        # Send request to change night (don't send nulls in JSON)
        args = locals()
        request = {k: v for k, v in args.items() if v is not None}
        status, msg = self.send_request(f"del_req {json.dumps(request)}")

        if status == "ERROR":
            self.log({"runtime_error": msg}, level="ERROR")
            raise RuntimeError(msg)

        # Return number of lines removed
        return msg
