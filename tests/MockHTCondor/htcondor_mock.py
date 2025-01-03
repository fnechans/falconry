# Mock HTCondor library for CI testing
# Developed with assistance from ChatGPT by OpenAI
# Provides simulated functionality for HTCondor, including
# job submission, querying, and management
from collections import defaultdict
import htcondor
import logging

log = logging.getLogger('falconry')


class MockSchedd:
    def __init__(self):
        self.job_queue = defaultdict(dict)
        self.job_history = defaultdict(dict)
        self.cluster_id_counter = 1
        self.log_files = {}  # Simulate log files per job

    def submit(self, job_description):
        """Simulates the submission of a job."""
        cluster_id = self.cluster_id_counter

        # Replace $(ClusterId) in the log file path
        log_file_path = job_description.get("Log", "mock_condor_$(ClusterId).log")
        log_file_path = log_file_path.replace("$(ClusterId)", str(cluster_id))

        # Create an initial log file
        self._write_log_file(log_file_path, "Job is idle.")

        self.job_queue[cluster_id] = {
            "JobDescription": job_description,
            "JobStatus": MockHTCondor.job_status_map()["Idle"],
        }
        self.cluster_id_counter += 1
        return MockSubmitResult(cluster_id)

    def get_constraint(self, constraint):
        if constraint is None:
            return lambda cluster_id, job_info: True
        else:
            if "==" in constraint and 'ClusterId' in constraint:
                constraint = constraint.split("==")
                return lambda cluster_id, job_info: cluster_id == int(
                    constraint[1].strip()
                )

    def query(self, constraint=None, projection=None):
        """Simulates querying the job queue."""
        # interpret constraint
        # for now only ==
        # TODO: add more
        l_constraint = self.get_constraint(constraint)

        result = []
        for cluster_id, job_info in self.job_queue.items():
            if l_constraint(cluster_id, job_info):
                if projection:
                    result.append({key: job_info.get(key, None) for key in projection})
                else:
                    result.append(job_info)
        return result

    def edit(self, cluster_id, attribute, value):
        """Simulates editing an attribute of a job."""
        if cluster_id in self.job_queue:
            self.job_queue[cluster_id][attribute] = value
        else:
            raise ValueError(f"Job ID {cluster_id} not found.")

    def remove(self, cluster_id):
        """Simulates removing a job from the queue."""
        if cluster_id in self.job_queue:
            job_info = self.job_queue[cluster_id]
            job_description = job_info["JobDescription"]
            log_file_path = job_description["Log"]
            log_file_path = log_file_path.replace("$(ClusterId)", str(cluster_id))
            self._write_log_file(log_file_path, "Job was aborted by the user.")
            self.job_history[cluster_id] = job_info
            del self.job_queue[cluster_id]
        else:
            raise ValueError(f"Job ID {cluster_id} not found.")

    def run_jobs(self):
        """Simulates running all idle jobs."""
        for cluster_id, job_info in self.job_queue.items():
            if job_info["JobStatus"] == MockHTCondor.job_status_map()["Idle"]:
                job_info["JobStatus"] = MockHTCondor.job_status_map()["Running"]
                log_file_path = job_info["JobDescription"]["Log"]
                log_file_path = log_file_path.replace("$(ClusterId)", str(cluster_id))
                self._write_log_file(log_file_path, "Job is running.")

    def complete_jobs(self):
        """Simulates completing all running jobs."""
        to_delete = []
        for cluster_id, job_info in self.job_queue.items():
            if job_info["JobStatus"] == MockHTCondor.job_status_map()["Running"]:
                job_info["JobStatus"] = MockHTCondor.job_status_map()["Completed"]
                log_file_path = job_info["JobDescription"]["Log"]
                log_file_path = log_file_path.replace("$(ClusterId)", str(cluster_id))
                self._write_log_file(
                    log_file_path, "Job terminated\nNormal termination (return value 0)"
                )
                self.job_history[cluster_id] = job_info
                to_delete.append(cluster_id)
        for cluster_id in to_delete:
            del self.job_queue[cluster_id]

    def fail_job(self, cluster_id, fail_code):
        """Simulates failing a specific job with a custom failure code."""
        if cluster_id in self.job_queue:
            job_info = self.job_queue[cluster_id]
            if job_info["JobStatus"] == MockHTCondor.job_status_map()["Running"]:
                job_info["JobStatus"] = MockHTCondor.job_status_map()["Completed"]
                log_file_path = job_info["JobDescription"]["Log"]
                log_file_path = log_file_path.replace("$(ClusterId)", str(cluster_id))
                self._write_log_file(
                    log_file_path,
                    f"Job terminated\nNormal termination (return value {fail_code})",
                )
                self.job_history[cluster_id] = job_info
                del self.job_queue[cluster_id]
        else:
            raise ValueError(f"Job ID {cluster_id} not found.")

    def act(self, action, constraint):
        """Simulates performing an action on jobs based on a constraint."""
        l_constraint = self.get_constraint(constraint)
        for cluster_id, job_info in list(self.job_queue.items()):
            if l_constraint(cluster_id, job_info):
                if action == htcondor.JobAction.Release:
                    if job_info["JobStatus"] == MockHTCondor.job_status_map()["Held"]:
                        job_info["JobStatus"] = MockHTCondor.job_status_map()["Idle"]
                        log_file_path = job_info["JobDescription"]["Log"]
                        log_file_path = log_file_path.replace(
                            "$(ClusterId)", str(cluster_id)
                        )
                        self._write_log_file(log_file_path, "Job is idle.")
                elif action == htcondor.JobAction.Remove:
                    self.remove(cluster_id)
                # Add more actions as needed

    def history(self, constraint=None, projection=None):
        """Simulates retrieving the history of completed jobs."""
        result = []
        l_constraint = self.get_constraint(constraint)
        for cluster_id, job_info in self.job_history.items():
            if job_info["JobStatus"] == MockHTCondor.job_status_map()["Completed"]:
                if l_constraint(cluster_id, job_info):
                    if projection:
                        result.append(
                            {key: job_info.get(key, None) for key in projection}
                        )
                    else:
                        result.append(job_info)
        return result

    def _write_log_file(self, log_file_path, content):
        """Writes content to the specified log file."""
        if log_file_path:
            with open(log_file_path, "w") as log_file:
                log_file.write(content)


class MockSubmitResult:
    def __init__(self, cluster_id):
        self.cluster_id = cluster_id

    def cluster(self):
        """Returns the cluster ID of the submitted job."""
        return self.cluster_id

    def __str__(self):
        return f"{self.cluster_id}"


class MockHTCondor:
    @staticmethod
    def Schedd():
        """Returns a mock Schedd object."""
        return MockSchedd()

    @staticmethod
    def job_status_map():
        """Provides a mapping of job statuses."""
        return {
            "Idle": 1,
            "Running": 2,
            "Completed": 4,
            "Removed": 3,
            "Held": 5,
        }
