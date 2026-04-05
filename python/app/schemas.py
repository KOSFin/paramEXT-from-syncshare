from typing import Any

from pydantic import BaseModel, Field


class ContextModel(BaseModel):
    host: str
    path: str
    fullUrl: str
    title: str = ''
    testKey: str


class OpenEduAnswerIn(BaseModel):
    answerKey: str
    answerText: str
    selected: bool = False
    correct: bool = False


class OpenEduQuestionIn(BaseModel):
    questionKey: str
    prompt: str = ''
    verified: bool = False
    isCorrect: bool = False
    answers: list[OpenEduAnswerIn] = Field(default_factory=list)


class OpenEduAttemptIn(BaseModel):
    source: str = 'extension'
    context: ContextModel
    completed: bool = False
    questions: list[OpenEduQuestionIn] = Field(default_factory=list)


class OpenEduSolutionsQueryIn(BaseModel):
    context: ContextModel
    questionKeys: list[str] = Field(default_factory=list)


class LogPayloadIn(BaseModel):
    kind: str
    payload: dict[str, Any] = Field(default_factory=dict)
    system: dict[str, Any] = Field(default_factory=dict)
