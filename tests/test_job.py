# Test job.py using the htcondor_mock library
from MockHTCondor import MockHTCondor
from falconry import job, manager, Counter
import pytest


def test_job():
    schedd = MockHTCondor.Schedd()
    j = job("test", schedd)
    j.set_simple("my_script.sh", "log")
    assert j.name == "test"
    assert j.schedd == schedd
    assert j.clusterIDs == []
    assert j.release() is False
    assert j.remove() is False
    assert j.submitted is False
    assert j.done is False
    assert j.skipped is False

    with pytest.raises(SystemError):
        j.get_info()

    j.submit()
    assert j.submitted is True
    assert len(j.clusterIDs) == 1
    assert j.clusterIDs[0] == 1
    assert j.clusterID == 1

    assert j.remove() is True
    assert j.get_info()["JobStatus"] == -999
    assert j.get_status() == 12

    j.submit()
    assert j.submitted
    assert len(j.clusterIDs) == 2
    assert j.clusterIDs[0] == 1
    assert j.clusterIDs[1] == 2
    assert j.clusterID == 2
    assert j.get_info()["JobStatus"] == 1
    assert j.get_info()["JobStatus"] == j.get_status()

    schedd.run_jobs()

    assert j.get_info()["JobStatus"] == 2
    assert j.get_info()["JobStatus"] == j.get_status()

    schedd.complete_jobs()

    assert j.get_info()["JobStatus"] == 4
    assert j.get_info()["JobStatus"] == j.get_status()


def test_manager():
    schedd = MockHTCondor.Schedd()
    mgr = manager("log")
    mgr.schedd = schedd

    j = job("test", schedd)
    j.set_simple("my_script.sh", "log")
    mgr.add_job(j)

    c = Counter()

    assert mgr._single_check(c) is True
    assert j.get_info()["JobStatus"] == 1
    schedd.run_jobs()
    assert mgr._single_check(c) is True
    assert j.get_info()["JobStatus"] == 2
    schedd.complete_jobs()
    assert mgr._single_check(c) is False
    assert j.get_info()["JobStatus"] == 4
    mgr.save(quiet=True)
    mgr.load(retryFailed=False)


if __name__ == "__main__":
    test_job()
    test_manager()
