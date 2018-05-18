from hashlib import sha256

from factory import Sequence
from factory.django import DjangoModelFactory
from factory.fuzzy import FuzzyDateTime, FuzzyText

from bec_alerts import models
from bec_alerts.utils import aware_datetime


class IssueFactory(DjangoModelFactory):
    class Meta:
        model = models.Issue

    fingerprint = Sequence(lambda n: sha256(str(n).encode()).hexdigest())
    last_seen = FuzzyDateTime(aware_datetime(2018, 1, 1))
    module = FuzzyText()
    stack_frames = []
    message = FuzzyText()
    groupId = '1'
