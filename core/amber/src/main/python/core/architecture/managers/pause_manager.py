from __future__ import annotations


class PauseManager:
    NoPause = 0
    Paused = 1

    def __init__(self):
        self.pause_privilege_level = PauseManager.NoPause

    def pause(self):
        self.pause_privilege_level = PauseManager.Paused

    def resume(self):
        self.pause_privilege_level = PauseManager.NoPause

    def is_paused(self) -> bool:
        return self.pause_privilege_level == PauseManager.Paused
