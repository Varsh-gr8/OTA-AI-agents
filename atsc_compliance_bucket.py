from datetime import datetime

# ---------------- COMPLIANCE STATES ----------------
COMPLIANCE_STATES = {
    "VERIFIED": "verified",
    "NORMALIZED": "normalized",
    "REJECTED": "rejected",
    "DEFERRED": "deferred"
}

# ---------------- ECU TARGETS ----------------
SUPPORTED_ECUS = {
    "braking", "engine_control", "powertrain", "transmission_control",
    "airbag_control", "suspension_control", "body_control",
    "climate_control", "telematics_control", "infotainment_control",
    "radar", "camera"
}

# ---------------- ATSC 3.0 PHY / SIGNALING ----------------
REQUIRED_MODULATION = "COFDM"
REQUIRED_CONSTELLATION = "NUC"
REQUIRED_FEC = {"LDPC", "BCH"}
REQUIRED_PROTOCOLS = {"ROUTE", "MMT"}

SNR_RANGE = (-5.5, 36.12)

# Demo-level object handling thresholds
MAX_OBJECT_SIZE = 512          # fragmentation threshold (demo abstraction)
MAX_URGENCY = 10
DEFAULT_CAROUSEL_MS = 5000


# =========================================================
# NECESSARY CHECKS (PHY + SIGNALING)
# =========================================================
def necessity_checks(campaign):
    errors = []

    # ---- Identity ----
    if not campaign.get("campaign_id"):
        errors.append("MISSING_CAMPAIGN_ID")

    if campaign.get("ecu_target") not in SUPPORTED_ECUS:
        errors.append("INVALID_ECU_TARGET")

    if campaign.get("size", 0) <= 0:
        errors.append("INVALID_PAYLOAD_SIZE")

    if not (1 <= campaign.get("urgency", 0) <= MAX_URGENCY):
        errors.append("INVALID_URGENCY")

    # ---- TX Profile Mandatory ----
    tx = campaign.get("tx_profile")
    if not isinstance(tx, dict):
        errors.append("MISSING_TX_PROFILE")
        return errors  # cannot proceed further

    # ---- PHY (A/322) ----
    if tx.get("modulation") != REQUIRED_MODULATION:
        errors.append("INVALID_MODULATION")

    if tx.get("constellation") != REQUIRED_CONSTELLATION:
        errors.append("INVALID_CONSTELLATION")

    fec = set(tx.get("fec", []))
    if not REQUIRED_FEC.issubset(fec):
        errors.append("FEC_NON_COMPLIANT")

    snr = tx.get("snr_db")
    if snr is None or not (SNR_RANGE[0] <= snr <= SNR_RANGE[1]):
        errors.append("SNR_OUT_OF_RANGE")

    # ---- Signaling / Delivery ----
    protocol = tx.get("protocol")
    if isinstance(protocol, list):
        if not REQUIRED_PROTOCOLS.intersection(protocol):
            errors.append("INVALID_DELIVERY_PROTOCOL")
    else:
        if protocol not in REQUIRED_PROTOCOLS:
            errors.append("INVALID_DELIVERY_PROTOCOL")

    plp_id = tx.get("plp_id")
    if plp_id is None or not isinstance(plp_id, int):
        errors.append("INVALID_PLP_ID")

    return errors


# =========================================================
# SUFFICIENT CHECKS (APP + SECURITY + INTEROP)
# =========================================================
def sufficient_checks(campaign):
    warnings = []
    app = campaign.get("app_profile", {})

    # ---- Media ----
    if app.get("codec") != "HEVC":
        warnings.append("NON_HEVC_CODEC")

    if app.get("audio") != "AC-4":
        warnings.append("NON_AC4_AUDIO")

    # ---- Security ----
    if not app.get("code_signed", False):
        warnings.append("UNSIGNED_APPLICATION")

    if app.get("drm", False) and not app.get("code_signed", False):
        warnings.append("DRM_WITHOUT_CODE_SIGNING")

    # ---- Emergency ----
    if not app.get("emergency_capable", False):
        warnings.append("AWARN_NOT_SUPPORTED")

    # ---- Object Handling (Demo abstraction) ----
    if campaign["size"] > MAX_OBJECT_SIZE:
        warnings.append("OBJECT_FRAGMENTATION_REQUIRED")

    if campaign["urgency"] < 3 and campaign["size"] > 400:
        warnings.append("LOW_PRIORITY_LARGE_OBJECT")

    return warnings


# =========================================================
# NORMALIZATION
# =========================================================
def normalize_campaign(campaign, warnings):
    if campaign["urgency"] > MAX_URGENCY:
        campaign["urgency"] = MAX_URGENCY

    if "OBJECT_FRAGMENTATION_REQUIRED" in warnings:
        campaign["fragmentation"] = {
            "enabled": True,
            "fragment_size": MAX_OBJECT_SIZE
        }

    return campaign


# =========================================================
# POLICY DECISION
# =========================================================
def should_defer(warnings):
    return "LOW_PRIORITY_LARGE_OBJECT" in warnings


# =========================================================
# METADATA ATTACHMENT
# =========================================================
def attach_atsc_metadata(campaign, state, warnings=None):
    tx = campaign["tx_profile"]

    campaign["atsc"] = {
        "service_id": f"SVC_{campaign['ecu_target'].upper()}",
        "plp_id": tx["plp_id"],
        "delivery_protocol": tx["protocol"],
        "broadcast_scope": "one-to-many",
        "carousel_interval_ms": DEFAULT_CAROUSEL_MS,
        "object_version": int(datetime.utcnow().timestamp()),
        "compliance_status": state,
        "warnings": warnings or []
    }
    return campaign


# =========================================================
# COMPLIANCE BUCKET (MAIN ENTRY)
# =========================================================
def atsc_compliance_bucket(campaign):

    # ---- NECESSARY ----
    errors = necessity_checks(campaign)
    if errors:
        campaign["atsc"] = {
            "compliance_status": COMPLIANCE_STATES["REJECTED"],
            "errors": errors
        }
        return campaign

    # ---- SUFFICIENT ----
    warnings = sufficient_checks(campaign)

    # ---- DEFERRAL POLICY ----
    if should_defer(warnings):
        return attach_atsc_metadata(
            campaign,
            COMPLIANCE_STATES["DEFERRED"],
            warnings
        )

    # ---- NORMALIZATION ----
    campaign = normalize_campaign(campaign, warnings)

    state = (
        COMPLIANCE_STATES["NORMALIZED"]
        if warnings else COMPLIANCE_STATES["VERIFIED"]
    )

    return attach_atsc_metadata(campaign, state, warnings)

