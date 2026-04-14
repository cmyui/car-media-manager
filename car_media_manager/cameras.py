import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Camera:
    source_name: str
    display_name: str
    volume_pattern: re.Pattern[str]
    media_extensions: frozenset[str]
    usb_pattern: re.Pattern[str] | None
    media_dir: str = "DCIM"

    def matches_volume_name(self, volume_name: str) -> bool:
        return self.volume_pattern.search(volume_name) is not None

    def _find_media_root(self, mount_path: Path) -> Path | None:
        if (mount_path / self.media_dir).is_dir():
            return mount_path / self.media_dir
        for sub in mount_path.iterdir():
            if sub.is_dir() and (sub / self.media_dir).is_dir():
                return sub / self.media_dir
        return None

    def matches_mount_structure(self, mount_path: Path) -> bool:
        return self._find_media_root(mount_path) is not None

    def scan(self, mount_path: Path) -> list[Path]:
        media_root = self._find_media_root(mount_path)
        if media_root is None:
            return []
        files: list[Path] = []
        for path in media_root.rglob("*"):
            if path.is_file() and path.suffix.lower() in self.media_extensions:
                files.append(path)
        return sorted(files)


GOPRO = Camera(
    source_name="gopro",
    display_name="GoPro",
    volume_pattern=re.compile(r"GoPro|HERO\d+", re.IGNORECASE),
    media_extensions=frozenset({".mp4", ".jpg", ".thm", ".lrv"}),
    usb_pattern=re.compile(r"GoPro", re.IGNORECASE),
)

INSTA360 = Camera(
    source_name="insta360",
    display_name="Insta360",
    volume_pattern=re.compile(r"Insta\s*360", re.IGNORECASE),
    media_extensions=frozenset({".mp4", ".insv", ".insp", ".jpg", ".lrv"}),
    usb_pattern=re.compile(r"Insta\s*360", re.IGNORECASE),
)

GOPRO_MAX = Camera(
    source_name="gopro_max",
    display_name="GoPro Max",
    volume_pattern=re.compile(r"MAX", re.IGNORECASE),
    media_extensions=frozenset({".mp4", ".360", ".jpg", ".thm", ".lrv"}),
    usb_pattern=re.compile(r"GoPro.*MAX", re.IGNORECASE),
)

GENERIC = Camera(
    source_name="generic",
    display_name="Unknown Camera",
    volume_pattern=re.compile(r"^$"),
    media_extensions=frozenset({".mp4", ".mov", ".jpg", ".jpeg", ".png", ".heic", ".insv", ".insp"}),
    usb_pattern=None,
)

KNOWN_CAMERAS: tuple[Camera, ...] = (GOPRO, GOPRO_MAX, INSTA360)


def identify_camera(mount_path: Path) -> Camera | None:
    volume_name = mount_path.name
    for camera in KNOWN_CAMERAS:
        if camera.matches_volume_name(volume_name):
            return camera
    if GENERIC.matches_mount_structure(mount_path):
        return GENERIC
    return None
