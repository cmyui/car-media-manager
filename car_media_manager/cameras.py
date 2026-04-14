import re
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Camera:
    source_name: str
    display_name: str
    uri_pattern: re.Pattern[str]
    media_extensions: frozenset[str]
    media_dir: str = "DCIM"

    def matches_uri(self, uri: str) -> bool:
        return self.uri_pattern.search(uri) is not None


GOPRO = Camera(
    source_name="gopro",
    display_name="GoPro",
    uri_pattern=re.compile(r"GoPro|HERO\d+", re.IGNORECASE),
    media_extensions=frozenset({".mp4", ".jpg", ".thm", ".lrv"}),
)

INSTA360 = Camera(
    source_name="insta360",
    display_name="Insta360",
    uri_pattern=re.compile(r"Insta\s*360", re.IGNORECASE),
    media_extensions=frozenset({".mp4", ".insv", ".insp", ".jpg", ".lrv"}),
)

GOPRO_MAX = Camera(
    source_name="gopro_max",
    display_name="GoPro Max",
    uri_pattern=re.compile(r"GoPro.*MAX|MAX.*GoPro", re.IGNORECASE),
    media_extensions=frozenset({".mp4", ".360", ".jpg", ".thm", ".lrv"}),
)

DJI_OSMO = Camera(
    source_name="dji",
    display_name="DJI Osmo",
    uri_pattern=re.compile(r"DJI|Osmo", re.IGNORECASE),
    media_extensions=frozenset({".mp4", ".jpg", ".dng"}),
)

GENERIC = Camera(
    source_name="generic",
    display_name="Unknown Camera",
    uri_pattern=re.compile(r"^$"),
    media_extensions=frozenset({".mp4", ".mov", ".jpg", ".jpeg", ".png", ".heic", ".insv", ".insp"}),
)

KNOWN_CAMERAS: tuple[Camera, ...] = (GOPRO, GOPRO_MAX, INSTA360, DJI_OSMO)


def identify_camera_from_uri(uri: str) -> Camera:
    for camera in KNOWN_CAMERAS:
        if camera.matches_uri(uri):
            return camera
    return GENERIC
