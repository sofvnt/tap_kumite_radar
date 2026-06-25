import cv2
import numpy as np
import datetime
import json
import math
from kafka import KafkaProducer
from ultralytics import YOLO

# ── CONFIG ──────────────────────────────────────────────────────────────────
MODEL_PATH     = "yolov8n.pt"
VIDEO_PATH     = "match.mp4"
OUTPUT_PATH    = "output_tracked.mp4"

MIN_AREA             = 4000
BORDER_MARGIN_RATIO  = 0.08
MIN_AREA_NEAR_BORDER = 8000
KNOWN_ATHLETE_MIN_AREA = 2000
MEMORY_FRAMES          = 45
MIN_COLOR_PIXELS       = 30

RED_LOWER1 = np.array([0,   80, 80]); RED_UPPER1 = np.array([10,  255, 255])
RED_LOWER2 = np.array([165, 80, 80]); RED_UPPER2 = np.array([180, 255, 255])
BLUE_LOWER = np.array([100, 80, 80]); BLUE_UPPER = np.array([130, 255, 255])

# ── STATO GLOBALE ────────────────────────────────────────────────────────────
memory = {
    "AKA": {"pos": None, "bbox": None, "lost_frames": 0, "confirmed": False},
    "AO":  {"pos": None, "bbox": None, "lost_frames": 0, "confirmed": False},
}
identity_votes = {"AKA": None, "AO": None}

# ── HELPERS ──────────────────────────────────────────────────────────────────
def bbox_area(box):
    x1, y1, x2, y2 = box
    return (x2 - x1) * (y2 - y1)

def bbox_center(box):
    x1, y1, x2, y2 = box
    return (int((x1 + x2) // 2), int((y1 + y2) // 2))

def is_near_lateral_border(box, frame_w, margin_ratio=BORDER_MARGIN_RATIO):
    x1, _, x2, _ = box
    margin = int(frame_w * margin_ratio)
    return x1 < margin or x2 > frame_w - margin

def geometric_filter(boxes, frame_w, frame_h, known_labels):
    kept = []
    for box in boxes:
        area = bbox_area(box)
        near_border = is_near_lateral_border(box, frame_w)
        cx = bbox_center(box)[0]
        is_known = False
        for label, state in known_labels.items():
            if state["confirmed"] and state["pos"] is not None:
                if abs(cx - state["pos"][0]) < frame_w * 0.20:
                    is_known = True
                    break
        if is_known:
            if area >= KNOWN_ATHLETE_MIN_AREA:
                kept.append(box)
        elif near_border:
            if area >= MIN_AREA_NEAR_BORDER:
                kept.append(box)
        else:
            if area >= MIN_AREA:
                kept.append(box)
    return kept

def count_color_pixels(frame, box, lower1, upper1, lower2=None, upper2=None):
    x1, y1, x2, y2 = [int(v) for v in box]
    
    # Analizza solo la fascia centrale del corpo (dal 30% all'80% dell'altezza)
    # Esclude testa/sfondo in alto e piedi/tatami in basso
    h = y2 - y1
    y1_crop = y1 + int(h * 0.30)
    y2_crop = y1 + int(h * 0.80)
    
    roi = frame[y1_crop:y2_crop, x1:x2]
    if roi.size == 0:
        return 0
    hsv = cv2.cvtColor(roi, cv2.COLOR_BGR2HSV)
    mask = cv2.inRange(hsv, lower1, upper1)
    if lower2 is not None:
        mask |= cv2.inRange(hsv, lower2, upper2)
    return int(np.sum(mask > 0))

def assign_identities(boxes, frame):
    global identity_votes
    if not boxes:
        return {"AKA": None, "AO": None}

    scores = []
    for box in boxes:
        red_px  = count_color_pixels(frame, box, RED_LOWER1, RED_UPPER1, RED_LOWER2, RED_UPPER2)
        blue_px = count_color_pixels(frame, box, BLUE_LOWER, BLUE_UPPER)
        scores.append({"box": box, "red": red_px, "blue": blue_px})

    result = {"AKA": None, "AO": None}

    if len(scores) == 1:
        s = scores[0]
        if s["red"] > s["blue"] and s["red"] >= MIN_COLOR_PIXELS:
            result["AKA"] = s["box"]
        elif s["blue"] > s["red"] and s["blue"] >= MIN_COLOR_PIXELS:
            result["AO"] = s["box"]
    else:
        aka_candidates = [s for s in scores
                          if s["red"] >= MIN_COLOR_PIXELS and s["red"] > s["blue"]]
        ao_candidates  = [s for s in scores
                          if s["blue"] >= MIN_COLOR_PIXELS and s["blue"] > s["red"]]

        if aka_candidates:
            best_aka = max(aka_candidates, key=lambda s: s["red"] - s["blue"])
            result["AKA"] = best_aka["box"]
            remaining = [s for s in ao_candidates if s is not best_aka]
            if remaining:
                result["AO"] = max(remaining, key=lambda s: s["blue"] - s["red"])["box"]
        elif ao_candidates:
            result["AO"] = max(ao_candidates, key=lambda s: s["blue"] - s["red"])["box"]

    # Stabilizzazione: blocca inversioni improvvise
    if (result["AKA"] is not None and result["AO"] is not None
            and identity_votes["AKA"] is not None):
        prev_cx = bbox_center(identity_votes["AKA"])[0]
        curr_aka_cx = bbox_center(result["AKA"])[0]
        curr_ao_cx  = bbox_center(result["AO"])[0]
        if abs(curr_aka_cx - prev_cx) > 200 and abs(curr_ao_cx - prev_cx) < 100:
            return {"AKA": identity_votes["AKA"], "AO": identity_votes["AO"]}

    if result["AKA"] is not None:
        identity_votes["AKA"] = result["AKA"]
    if result["AO"] is not None:
        identity_votes["AO"] = result["AO"]

    return result

def update_memory(assigned, frame_w):
    for label in ["AKA", "AO"]:
        box = assigned.get(label)
        if box is not None:
            cx, cy = bbox_center(box)
            memory[label]["pos"]         = (cx, cy)
            memory[label]["bbox"]        = box
            memory[label]["lost_frames"] = 0
            memory[label]["confirmed"]   = True
        else:
            memory[label]["lost_frames"] += 1

def draw_overlay(frame, assigned):
    colors = {"AKA": (0, 0, 220), "AO": (220, 100, 0)}
    status_aka = "OK"
    status_ao  = "OK"
    pos_aka = None
    pos_ao  = None

    for label in ["AKA", "AO"]:
        box = assigned.get(label)
        col = colors[label]
        if box is not None:
            x1, y1, x2, y2 = [int(v) for v in box]
            cv2.rectangle(frame, (x1, y1), (x2, y2), col, 2)
            cv2.putText(frame, label, (x1, y1 - 8),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, col, 2)
            cx, cy = bbox_center(box)
            if label == "AKA": pos_aka = (cx, cy)
            else:               pos_ao  = (cx, cy)
        else:
            mem = memory[label]
            if mem["pos"] is not None:
                cx, cy = mem["pos"]
                if label == "AKA":
                    pos_aka   = (cx, cy)
                    status_ao = f"AO (AKA Fuori f{mem['lost_frames']})"
                else:
                    pos_ao    = (cx, cy)
                    status_aka = f"AKA (AO Fuori f{mem['lost_frames']})"
                cv2.circle(frame, (cx, cy), 30, col, 2)
                cv2.putText(frame, f"{label}?", (cx - 20, cy - 35),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.6, col, 1)

    if pos_aka and pos_ao:
        cv2.line(frame, pos_aka, pos_ao, (0, 220, 0), 2)
        dist = int(np.hypot(pos_ao[0] - pos_aka[0], pos_ao[1] - pos_aka[1]))
        mid  = ((pos_aka[0] + pos_ao[0]) // 2, (pos_aka[1] + pos_ao[1]) // 2)
        cv2.putText(frame, f"Maai: {dist}px", (mid[0] - 40, mid[1] - 10),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.65, (0, 220, 0), 2)

    cv2.putText(frame, status_aka, (10, 30),
                cv2.FONT_HERSHEY_SIMPLEX, 0.65, colors["AKA"], 2)
    cv2.putText(frame, status_ao, (10, 58),
                cv2.FONT_HERSHEY_SIMPLEX, 0.65, colors["AO"], 2)
    return frame

# ── MAIN LOOP ────────────────────────────────────────────────────────────────
def main():
    # 1. Inizializzazione del Tubo Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("[OK] Produttore Kafka pronto e connesso!")
    except Exception as e:
        print(f"[ERRORE] Impossibile avviare Kafka: {e}")
        return

    model  = YOLO(MODEL_PATH)
    cap    = cv2.VideoCapture(VIDEO_PATH)
    fps    = cap.get(cv2.CAP_PROP_FPS) or 30
    fw     = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    fh     = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    writer = cv2.VideoWriter(OUTPUT_PATH, cv2.VideoWriter_fourcc(*"mp4v"),
                             fps, (fw, fh))

    while True:
        ret, frame = cap.read()
        if not ret:
            break
            
        results   = model(frame, classes=[0], verbose=False)[0]
        raw_boxes = [box.xyxy[0].cpu().numpy()
                     for box in results.boxes
                     if float(box.conf[0]) > 0.35]
                     
        filtered  = geometric_filter(raw_boxes, fw, fh, memory)
        filtered  = sorted(filtered, key=bbox_area, reverse=True)[:3]
        assigned  = assign_identities(filtered, frame)
        update_memory(assigned, fw)
        
        # Disegna la grafica a schermo
        frame = draw_overlay(frame, assigned)

        # ── KAFKA & KIBANA BRIDGE ────────────────────────────────────────────
        # Recuperiamo le posizioni dalla memoria globale appena aggiornata
        pos_aka = memory["AKA"]["pos"]
        pos_ao  = memory["AO"]["pos"]

        if pos_aka is not None and pos_ao is not None:
            # Ricalcoliamo la distanza per il database
            distanza = math.hypot(pos_ao[0] - pos_aka[0], pos_ao[1] - pos_aka[1])
            
            # Creiamo il pacchetto con il TIMESTAMP per Kibana
            payload_dati = {
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "atleta_aka_x": int(pos_aka[0]),
                "atleta_aka_y": int(pos_aka[1]),
                "atleta_ao_x": int(pos_ao[0]),
                "atleta_ao_y": int(pos_ao[1]),
                "distanza": float(distanza)
            }
            # Invio effettivo
            producer.send('kumite_topic', value=payload_dati)
        # ─────────────────────────────────────────────────────────────────────

        writer.write(frame)
        cv2.imshow("Karate Tracker", frame)
        if cv2.waitKey(1) & 0xFF == ord("q"):
            break

    # Chiusura pulita delle risorse
    cap.release()
    writer.release()
    producer.flush()
    producer.close()
    cv2.destroyAllWindows()

if __name__ == "__main__":
    main()