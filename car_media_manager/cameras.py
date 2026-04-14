import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Camera:
    source_name: str
    display_name: str
    mount_pattern: re.Pattern[str]
    media_extensions: frozenset[str]
    media_dir: str = "DCIM"

    def matches_mount(self, mount_path: Path) -> bool:
        return self.mount_pattern.search(mount_path.name) is not None

    def find_media_root(self, mount_path: Path) -> Path | None:
        if (mount_path / self.media_dir).is_dir():
            return mount_path / self.media_dir
        for sub in mount_path.iterdir():
            if sub.is_dir() and (sub / self.media_dir).is_dir():
                return sub / self.media_dir
        return None

    def scan(self, mount_path: Path) -> list[Path]:
        media_root = self.find_media_root(mount_path)
        if media_root is None:
            return []
        return sorted(
            p
            for p in media_root.rglob("*")
            if p.is_file()
            and not p.name.startswith(".")
            and p.suffix.lower() in self.media_extensions
        )


GOPRO = Camera(
    source_name="gopro",
    display_name="GoPro",
    mount_pattern=re.compile(r"GoPro|HERO\d+", re.IGNORECASE),
    media_extensions=frozenset({".mp4", ".jpg", ".thm", ".lrv"}),
)

GOPRO_MAX = Camera(
    source_name="gopro_max",
    display_name="GoPro Max",
    mount_pattern=re.compile(r"GoPro.*MAX|MAX.*GoPro", re.IGNORECASE),
    media_extensions=frozenset({".mp4", ".360", ".jpg", ".thm", ".lrv"}),
)

INSTA360 = Camera(
    source_name="insta360",
    display_name="Insta360",
    mount_pattern=re.compile(r"Insta\s*360", re.IGNORECASE),
    media_extensions=frozenset({".mp4", ".insv", ".insp", ".jpg", ".lrv"}),
)

DJI_OSMO = Camera(
    source_name="dji",
    display_name="DJI Osmo",
    mount_pattern=re.compile(r"DJI|Osmo", re.IGNORECASE),
    media_extensions=frozenset({".mp4", ".jpg", ".dng"}),
)

GENERIC = Camera(
    source_name="generic",
    display_name="Unknown Camera",
    mount_pattern=re.compile(r"^$"),
    media_extensions=frozenset({".mp4", ".mov", ".jpg", ".jpeg", ".png", ".heic", ".insv", ".insp"}),
)

KNOWN_CAMERAS: tuple[Camera, ...] = (GOPRO, GOPRO_MAX, INSTA360, DJI_OSMO)


def identify_camera(mount_path: Path) -> Camera | None:
    for camera in KNOWN_CAMERAS:
        if camera.matches_mount(mount_path):
            return camera
    if GENERIC.find_media_root(mount_path) is not None:
        return GENERIC
    return None
