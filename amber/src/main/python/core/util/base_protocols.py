# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from abc import abstractmethod
from typing import TypeVar, Sized, Optional
from typing_extensions import Protocol

T = TypeVar("T")
K = TypeVar("K")


class Putable(Protocol):
    @abstractmethod
    def put(self, item: T) -> None:
        pass


class KeyedPutable(Protocol):
    @abstractmethod
    def put(self, key: K, item: T) -> None:
        pass


class Getable(Protocol):
    @abstractmethod
    def get(self) -> T:
        pass


class FlushedGetable(Protocol):
    @abstractmethod
    def get(self, flush: bool) -> T:
        pass


class EmtpyCheckable(Sized):
    @abstractmethod
    def is_empty(self) -> bool:
        pass


class KeyedEmtpyCheckable(Sized):
    @abstractmethod
    def is_empty(self, key: Optional[K] = None) -> bool:
        pass
