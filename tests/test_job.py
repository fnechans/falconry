# Test job.py using the htcondor_mock library
from MockHTCondor import MockHTCondor
from falconry import job, manager, Counter, FalconryStatus
import pytest


def test_job():
    schedd = MockHTCondor.Schedd()
    j = job("test", schedd)  # type: ignore
    j.set_simple("my_script.sh", "log")
    assert j.name == "test"
    assert j.schedd == schedd
    assert j.jobIDs == []
    assert j.release() is False
    assert j.remove() is False
    assert j.submitted is False
    assert j.done is False
    assert j.skipped is False

    with pytest.raises(SystemError):
        j.get_info()

    j.submit()
    assert j.submitted is True
    assert len(j.jobIDs) == 1
    assert j.jobIDs[0] == "1.0"
    assert j.jobID == "1.0"

    assert j.remove() is True
    assert j.get_info()["JobStatus"] == -999
    assert j.get_status() == FalconryStatus.ABORTED_BY_USER

    j.submit()
    assert j.submitted
    assert len(j.jobIDs) == 2
    assert j.jobIDs[0] == "1.0"
    assert j.jobIDs[1] == "2.0"
    assert j.jobID == "2.0"
    assert j.get_status() == FalconryStatus.IDLE
    assert j.get_info()["JobStatus"] == j.get_status().value

    schedd.run_jobs()

    assert j.get_status() == FalconryStatus.RUNNING
    assert j.get_info()["JobStatus"] == j.get_status().value

    schedd.complete_jobs()

    assert j.get_status() == FalconryStatus.COMPLETE
    assert j.get_info()["JobStatus"] == j.get_status().value


def test_manager():
    schedd = MockHTCondor.Schedd()
    mgr = manager("log", schedd=schedd)  # type: ignore

    j = job("test", schedd)  # type: ignore
    j.set_simple("my_script.sh", "log")
    mgr.add_job(j)

    c = Counter()

    assert mgr._single_check(c) is True
    mgr._submit_jobs()
    assert j.get_status() == FalconryStatus.IDLE
    schedd.run_jobs()
    assert mgr._single_check(c) is True
    assert j.get_status() == FalconryStatus.RUNNING
    schedd.complete_jobs()
    assert mgr._single_check(c) is False
    assert j.get_status() == FalconryStatus.COMPLETE
    mgr.save(quiet=True)
    mgr.load(retryFailed=False)


if __name__ == "__main__":
    test_job()
    test_manager()
