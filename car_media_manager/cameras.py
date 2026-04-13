import re
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Camera:
    source_name: str
    display_name: str
    volume_names: frozenset[str]
    media_extensions: frozenset[str]
    usb_pattern: re.Pattern[str] | None
    media_root_dirs: frozenset[str] = field(default_factory=lambda: frozenset({"DCIM"}))

    def matches_volume_name(self, volume_name: str) -> bool:
        return volume_name in self.volume_names

    def matches_mount_structure(self, mount_path: Path) -> bool:
        return any((mount_path / root).is_dir() for root in self.media_root_dirs)

    def scan(self, mount_path: Path) -> list[Path]:
        files: list[Path] = []
        for root in self.media_root_dirs:
            root_path = mount_path / root
            if not root_path.is_dir():
                continue
            for path in root_path.rglob("*"):
                if path.is_file() and path.suffix.lower() in self.media_extensions:
                    files.append(path)
        return sorted(files)


GOPRO = Camera(
    source_name="gopro",
    display_name="GoPro",
    volume_names=frozenset({"HERO13 BLACK", "HERO12 BLACK", "HERO11 BLACK", "HERO10 BLACK"}),
    media_extensions=frozenset({".mp4", ".jpg", ".thm", ".lrv"}),
    usb_pattern=re.compile(r"GoPro", re.IGNORECASE),
)

INSTA360 = Camera(
    source_name="insta360",
    display_name="Insta360",
    volume_names=frozenset({"Insta360 X4", "Insta360 X3", "Insta360 X2", "Insta360 X5"}),
    media_extensions=frozenset({".mp4", ".insv", ".insp", ".jpg", ".lrv"}),
    usb_pattern=re.compile(r"Insta\s*360", re.IGNORECASE),
)

GENERIC = Camera(
    source_name="generic",
    display_name="Unknown Camera",
    volume_names=frozenset(),
    media_extensions=frozenset({".mp4", ".mov", ".jpg", ".jpeg", ".png", ".heic", ".insv", ".insp"}),
    usb_pattern=None,
)

KNOWN_CAMERAS: tuple[Camera, ...] = (GOPRO, INSTA360)


def identify_camera(mount_path: Path) -> Camera | None:
    volume_name = mount_path.name
    for camera in KNOWN_CAMERAS:
        if camera.matches_volume_name(volume_name):
            return camera
    if GENERIC.matches_mount_structure(mount_path):
        return GENERIC
    return None
