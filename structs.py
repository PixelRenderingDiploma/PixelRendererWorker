from dataclasses import dataclass

@dataclass
class RenderingSettings:
    type: int
    flyby: int
    scene_effect: int
    post_effect: int

    duration: int
    start_frame: int

@dataclass
class RenderingRequest:
    id: str
    id_token: str
    id_model: str
    status: str
    settings: RenderingSettings