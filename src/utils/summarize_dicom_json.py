#!/usr/bin/env python3
"""
Walk out_dicom/0001, read all header.json files, and build a consolidated
hierarchy: Study → Series → Instances, including basic attributes and
ReferencedImageSequence links.

Outputs a summary JSON file and optionally prints a concise textual summary.

Usage:
  python3 src/summarize_dicom_json.py --root out_dicom/0001 --out out_dicom/summary.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional


KEYS = {
    "StudyInstanceUID": "(0020,000D) StudyInstanceUID",
    "SeriesInstanceUID": "(0020,000E) SeriesInstanceUID",
    "SOPInstanceUID": "(0008,0018) SOPInstanceUID",
    "SeriesNumber": "(0020,0011) SeriesNumber",
    "InstanceNumber": "(0020,0013) InstanceNumber",
    "SeriesDescription": "(0008,103E) SeriesDescription",
    "Modality": "(0008,0060) Modality",
    "BodyPartExamined": "(0018,0015) BodyPartExamined",
    "PixelSpacing": "(0028,0030) PixelSpacing",
    "SliceThickness": "(0018,0050) SliceThickness",
    "SpacingBetweenSlices": "(0018,0088) SpacingBetweenSlices",
    "ImageOrientationPatient": "(0020,0037) ImageOrientationPatient",
    "ImagePositionPatient": "(0020,0032) ImagePositionPatient",
    # Patient-level (to include a bit of context):
    "PatientSex": "(0010,0040) PatientSex",
    "PatientAge": "(0010,1010) PatientAge",
}


def load_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        sys.stderr.write(f"Failed to read {path}: {e}\n")
        return None


def get(d: Dict[str, Any], key: str) -> Optional[str]:
    return d.get(key)


def collect_headers(root: str) -> List[str]:
    headers: List[str] = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for fn in filenames:
            if fn == "header.json":
                headers.append(os.path.join(dirpath, fn))
    headers.sort()
    return headers


def infer_plane(orientation_str: Optional[str]) -> Optional[str]:
    """Best-effort identify plane (axial/coronal/sagittal) from IOP string like "[a,b,c,d,e,f]".

    Normal vector = row × column. If normal close to:
      - ±X → sagittal
      - ±Y → coronal
      - ±Z → axial
    """
    if not orientation_str:
        return None
    try:
        # orientation comes as a string like "[1, 0, 0, 0, 1, 0]"
        nums = orientation_str.strip().strip("[]").split(",")
        vals = [float(x) for x in nums]
        if len(vals) != 6:
            return None
        rx, ry, rz, cx, cy, cz = vals
        # cross product r x c
        nx = ry * cz - rz * cy
        ny = rz * cx - rx * cz
        nz = rx * cy - ry * cx
        # Determine dominant axis
        abs_vals = [(abs(nx), "sagittal"), (abs(ny), "coronal"), (abs(nz), "axial")]
        plane = max(abs_vals, key=lambda t: t[0])[1]
        return plane
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="out_dicom/0001", help="Root folder to scan")
    ap.add_argument("--out", default="out_dicom/summary.json", help="Output JSON path")
    ap.add_argument("--print", dest="do_print", action="store_true", help="Print concise summary")
    args = ap.parse_args()

    headers = collect_headers(args.root)
    if not headers:
        print(f"No header.json files found under {args.root}")
        return

    studies: Dict[str, Any] = {}
    # Map instance UID → metadata path for cross-ref resolution
    instance_index: Dict[str, Dict[str, Any]] = {}

    patient_meta = {"PatientSex": None, "PatientAge": None}

    for hp in headers:
        data = load_json(hp)
        if not data:
            continue
        study_uid = get(data, KEYS["StudyInstanceUID"]) or "unknown-study"
        series_uid = get(data, KEYS["SeriesInstanceUID"]) or "unknown-series"
        sop_uid = get(data, KEYS["SOPInstanceUID"]) or os.path.basename(os.path.dirname(hp))

        series_number = get(data, KEYS["SeriesNumber"]) or ""
        instance_number = get(data, KEYS["InstanceNumber"]) or ""
        series_desc = get(data, KEYS["SeriesDescription"]) or ""
        modality = get(data, KEYS["Modality"]) or ""
        body_part = get(data, KEYS["BodyPartExamined"]) or ""
        iop = get(data, KEYS["ImageOrientationPatient"]) or None
        plane = infer_plane(iop)

        ref_uids: List[str] = []
        # ReferencedImageSequence handling:
        # Some exports store it as a list at key '(0008,1140) ReferencedImageSequence'.
        # Others flatten to multiple keys like '(0008,1140) ReferencedImageSequence #1', '#2', etc.
        for k, v in data.items():
            if k.startswith("(0008,1140) ReferencedImageSequence"):
                if isinstance(v, list):
                    for item in v:
                        if isinstance(item, dict):
                            ref = item.get("(0008,1155) ReferencedSOPInstanceUID")
                            if isinstance(ref, str) and ref:
                                ref_uids.append(ref)
                elif isinstance(v, dict):
                    ref = v.get("(0008,1155) ReferencedSOPInstanceUID")
                    if isinstance(ref, str) and ref:
                        ref_uids.append(ref)
        # Some tools also include a top-level '(0008,1155) ReferencedSOPInstanceUID'
        top_ref = data.get("(0008,1155) ReferencedSOPInstanceUID")
        if isinstance(top_ref, str) and top_ref:
            ref_uids.append(top_ref)
        # De-duplicate while preserving order
        seen = set()
        ref_uids = [x for x in ref_uids if not (x in seen or seen.add(x))]

        # Update patient meta (best-effort; consistent across files in a study)
        if not patient_meta["PatientSex"]:
            patient_meta["PatientSex"] = get(data, KEYS["PatientSex"]) or patient_meta["PatientSex"]
        if not patient_meta["PatientAge"]:
            patient_meta["PatientAge"] = get(data, KEYS["PatientAge"]) or patient_meta["PatientAge"]

        study = studies.setdefault(study_uid, {
            "Patient": patient_meta,
            "Series": {}
        })
        series = study["Series"].setdefault(series_uid, {
            "SeriesNumber": series_number,
            "SeriesDescription": series_desc,
            "Modality": modality,
            "BodyPartExamined": body_part,
            "Instances": [],
        })
        series["Instances"].append({
            "SOPInstanceUID": sop_uid,
            "InstanceNumber": instance_number,
            "ReferencedSOPInstanceUIDs": ref_uids,
            "ImageOrientationPatient": iop,
            "InferredPlane": plane,
        })

        # index instance
        instance_index[sop_uid] = {
            "StudyInstanceUID": study_uid,
            "SeriesInstanceUID": series_uid,
            "InstanceNumber": instance_number,
            "SeriesDescription": series_desc,
        }

    # Sort instances by numeric InstanceNumber where possible
    for study in studies.values():
        for series in study["Series"].values():
            def inst_key(it: Dict[str, Any]) -> Any:
                try:
                    return int(str(it.get("InstanceNumber") or "0"))
                except Exception:
                    return str(it.get("InstanceNumber") or "")
            series["Instances"].sort(key=inst_key)
            series["InstanceCount"] = len(series["Instances"])

    # Add cross-reference resolution summary at series level (counts only)
    for study in studies.values():
        for series in study["Series"].values():
            ref_count = 0
            unresolved = 0
            for inst in series["Instances"]:
                for rid in inst.get("ReferencedSOPInstanceUIDs", []) or []:
                    ref_count += 1
                    if rid not in instance_index:
                        unresolved += 1
            series["ReferenceCounts"] = {"total": ref_count, "unresolved": unresolved}

    # Persist JSON
    out_path = args.out
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"studies": studies}, f, indent=2, ensure_ascii=False)

    # Optional concise print
    if args.do_print:
        study_count = len(studies)
        series_total = sum(len(study["Series"]) for study in studies.values())
        inst_total = sum(sum(s["InstanceCount"] for s in study["Series"].values()) for study in studies.values())
        print(f"Studies: {study_count}, Series: {series_total}, Instances: {inst_total}")
        for s_uid, study in studies.items():
            print(f"Study {s_uid} → {len(study['Series'])} series")
            for ser_uid, ser in study["Series"].items():
                desc = ser.get("SeriesDescription") or ""
                num = ser.get("SeriesNumber") or ""
                ic = ser.get("InstanceCount")
                refs = ser.get("ReferenceCounts", {})
                print(f"  Series {num} {desc} ({ser_uid}) → {ic} instances, refs: {refs}")


if __name__ == "__main__":
    main()
