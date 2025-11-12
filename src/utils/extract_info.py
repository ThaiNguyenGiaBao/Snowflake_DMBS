#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import re
from pathlib import Path

import numpy as np
from PIL import Image
import pydicom
from pydicom.pixel_data_handlers.util import apply_modality_lut


INPUT_DIR = "/Users/baothainguyengia/Desktop/[btl]DBMS/01_MRI_Data"
OUTPUT_DIR = "/Users/baothainguyengia/Desktop/[btl]DBMS/out_dicom"   # header.json + image.png written here
SAVE_16BIT_PNG = False  # True => save 16-bit PNG (no windowing); False => 8-bit with window/minmax


def slugify(s: str) -> str:
    s = s.strip().replace(os.sep, "_")
    return re.sub(r"[^\w\-\.]+", "_", s)[:255] or "unnamed"

def to_jsonable(v):
    if isinstance(v, (str, int, float)) or v is None: return v
    if isinstance(v, (bytes, bytearray)): return v.decode(errors="ignore")
    if isinstance(v, (list, tuple)): return [to_jsonable(x) for x in v]
    try: return str(v)
    except Exception: return "<unserializable>"
    
def key_with_tag(elem) -> str:
    """Format key as '(GGGG,EEEE) Keyword' (Keyword has no spaces)."""
    kw = elem.keyword or (elem.name or "").replace(" ", "")
    return f"({elem.tag.group:04X},{elem.tag.element:04X}) {kw}"

def value_to_string(v) -> str:
    """Return a string for JSON (lists -> their repr as a string, bytes decoded)."""
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        return v.decode(errors="ignore")
    # MultiValue/list/tuple: keep as a single string (e.g. "['A','B']")
    if isinstance(v, (list, tuple)):
        try:
            # shorten if the length is
            return str(list(v))
        except Exception:
            return str([str(x) for x in v])
    # Most DICOM types stringify nicely (PersonName, DA/TM/DT, IS/DS, UID, etc.)
    try:
        return str(v)
    except Exception:
        return "<unserializable>"

def ds_header_to_dict(ds: pydicom.Dataset) -> dict:
    """
    Build a dict of ALL elements except PixelData.
    - Non-sequence values are stored as STRINGS.
    - Sequence (SQ) values are lists of dicts, recursively using the same formatting.
    """
    out = {}
    for elem in ds.iterall():
        # skip raw pixels
        if elem.tag == (0x7FE0, 0x0010) or elem.tag == (0xFFFC,0xFFFC):  
            continue

        k = key_with_tag(elem)

        if elem.VR == "SQ":  # a list of Datasets
            items = []
            for item in elem:  # each 'item' is a Dataset
                items.append(ds_header_to_dict(item))
            out[k] = items
        else:
            out[k] = value_to_string(elem.value)
    return out

def get_window(ds):
    wc = getattr(ds, "WindowCenter", None)
    ww = getattr(ds, "WindowWidth", None)
    try: wc = float(wc[0] if hasattr(wc, "__iter__") else wc)
    except: wc = None
    try: ww = float(ww[0] if hasattr(ww, "__iter__") else ww)
    except: ww = None
    return (wc, ww) if (wc is not None and ww and ww > 0) else None

def window_to_uint8(img, wc, ww):
    lo, hi = wc - ww/2.0, wc + ww/2.0
    img = np.clip(img, lo, hi)
    return ((img - lo) / (hi - lo + 1e-9) * 255.0).astype(np.uint8)

def scale_minmax_to_uint8(img):
    img = img.astype(np.float32)
    mn, mx = float(img.min()), float(img.max())
    if mx <= mn: return np.zeros_like(img, dtype=np.uint8)
    return ((img - mn) / (mx - mn) * 255.0).astype(np.uint8)

def save_image_frames(ds, out_dir: Path, base_name: str):
    arr = apply_modality_lut(ds.pixel_array, ds) if "PixelData" in ds else None
    if arr is None: return []
    arr = arr.astype(np.float32, copy=False)
    wcww = get_window(ds)
    photometric = getattr(ds, "PhotometricInterpretation", "MONOCHROME2").upper()

    saved = []
    def save_frame(frame, suffix: str):
        if SAVE_16BIT_PNG:
            img = np.clip(frame, 0, np.iinfo(np.uint16).max).astype(np.uint16)
            if photometric == "MONOCHROME1": img = np.iinfo(np.uint16).max - img
            Image.fromarray(img, mode="I;16").save(out_dir / f"{base_name}{suffix}.png")
        else:
            img = window_to_uint8(frame, *wcww) if wcww else scale_minmax_to_uint8(frame)
            if photometric == "MONOCHROME1": img = 255 - img
            Image.fromarray(img, mode="L").save(out_dir / f"{base_name}{suffix}.png")
        saved.append(out_dir / f"{base_name}{suffix}.png")

    if arr.ndim == 3:
        for i in range(arr.shape[0]):
            save_frame(arr[i], suffix=f"_f{i:03d}")
    else:
        save_frame(arr, suffix="")
    return saved

def process_folder(in_dir, out_dir):
    in_dir, out_dir = Path(in_dir), Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    exts = {".dcm", ".ima"}

    for f in sorted(in_dir.rglob("*")):
        if not f.is_file() or f.suffix.lower() not in exts:
            continue
        try:
            ds = pydicom.dcmread(str(f), force=True)
        except Exception as e:
            print(f"✘ Read failed: {f} -> {e}")
            continue

        # Rebuild the input directory structure under OUTPUT_DIR
        rel_dir = f.parent.relative_to(in_dir)        # e.g. LOCALIZER_0001/
        folder_name = slugify(getattr(ds, "SOPInstanceUID", "") or f.stem)
        inst_dir = out_dir / rel_dir / folder_name    # e.g. out/LOCALIZER_0001/<SOP_UID>/
        inst_dir.mkdir(parents=True, exist_ok=True)

        # 1) header.json
        try:
            header = ds_header_to_dict(ds)
            with open(inst_dir / "header.json", "w", encoding="utf-8") as fp:
                json.dump(header, fp, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"✘ Header JSON failed: {f} -> {e}")

        # 2) image(s)
        try:
            if "PixelData" in ds:
                outs = save_image_frames(ds, inst_dir, base_name="image")
                print(f"✔ {f.relative_to(in_dir)} → {inst_dir} ({len(outs)} PNG)")
            else:
                print(f"• {f.relative_to(in_dir)} has no PixelData")
        except Exception as e:
            print(f"✘ Image save failed: {f} -> {e}")

if __name__ == "__main__":
    process_folder(INPUT_DIR, OUTPUT_DIR)
