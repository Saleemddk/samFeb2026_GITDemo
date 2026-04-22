"""
Cement Fineness Prediction — Advanced ML Pipeline
Predicts Res45_AVG (45μm sieve residue %) from cement mill sensor data.
Uses feature engineering + multiple models to beat the baseline 95.21% accuracy.
"""

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

from sklearn.linear_model import Ridge, Lasso, ElasticNet
from sklearn.ensemble import (
    RandomForestRegressor,
    GradientBoostingRegressor,
    StackingRegressor,
)
from sklearn.svm import SVR
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    mean_absolute_percentage_error,
)
from sklearn.model_selection import cross_val_score
import os

# Try importing XGBoost/LightGBM — optional
try:
    from xgboost import XGBRegressor
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
    print("⚠ xgboost not installed — skipping XGBoost model")

try:
    from lightgbm import LGBMRegressor
    HAS_LGBM = True
except ImportError:
    HAS_LGBM = False
    print("⚠ lightgbm not installed — skipping LightGBM model")


# ============================================================================
# 1. LOAD DATA
# ============================================================================
print("=" * 70)
print("CEMENT FINENESS PREDICTION — ADVANCED ML PIPELINE")
print("=" * 70)

DATA_PATH = os.path.expanduser("~/Downloads/Python Output Data.xlsx")
df_full = pd.read_excel(DATA_PATH, sheet_name="Final Clean Data")
df_train_raw = pd.read_excel(DATA_PATH, sheet_name="train")
df_test_raw = pd.read_excel(DATA_PATH, sheet_name="test")

print(f"\nData loaded: Full={len(df_full)}, Train={len(df_train_raw)}, Test={len(df_test_raw)}")

FEATURES = [
    "Sound level", "Production Rate", "Gypsum Feed",
    "Sepol Drive Speed", "Separator Fan", "Mill Feed Rate",
    "Bucket Elev", "Main Motor",
]
TARGET = "Res45_AVG"


# ============================================================================
# 2. FEATURE ENGINEERING
# ============================================================================
def engineer_features(df, is_train=True, train_stats=None):
    """Create advanced features from raw sensor data."""
    df = df.copy()
    df = df.sort_values("DateTime").reset_index(drop=True)

    # --- Rolling window features (capture trends) ---
    for col in FEATURES:
        df[f"{col}_roll4_mean"] = df[col].rolling(window=2, min_periods=1).mean()
        df[f"{col}_roll8_mean"] = df[col].rolling(window=4, min_periods=1).mean()
        df[f"{col}_roll4_std"] = df[col].rolling(window=2, min_periods=1).std().fillna(0)

    # --- Rate of change (sensor derivatives) ---
    for col in FEATURES:
        df[f"{col}_diff"] = df[col].diff().fillna(0)

    # --- Lag features (past readings affect current fineness) ---
    for col in FEATURES:
        df[f"{col}_lag1"] = df[col].shift(1)
        df[f"{col}_lag2"] = df[col].shift(2)

    # --- Interaction features (key physical relationships) ---
    df["MillFeed_x_SepFan"] = df["Mill Feed Rate"] * df["Separator Fan"]
    df["MainMotor_x_Sound"] = df["Main Motor"] * df["Sound level"]
    df["MillFeed_x_BucketElev"] = df["Mill Feed Rate"] * df["Bucket Elev"]
    df["ProdRate_x_GypsumFeed"] = df["Production Rate"] * df["Gypsum Feed"]
    df["SepolSpeed_x_SepFan"] = df["Sepol Drive Speed"] * df["Separator Fan"]
    df["MainMotor_per_MillFeed"] = df["Main Motor"] / df["Mill Feed Rate"].replace(0, np.nan)
    df["Sound_per_MillFeed"] = df["Sound level"] / df["Mill Feed Rate"].replace(0, np.nan)

    # --- Ratio features ---
    df["BucketElev_per_MillFeed"] = df["Bucket Elev"] / df["Mill Feed Rate"].replace(0, np.nan)
    df["ProdRate_per_MainMotor"] = df["Production Rate"] / df["Main Motor"].replace(0, np.nan)

    # --- Time features ---
    if "DateTime" in df.columns:
        df["hour"] = df["DateTime"].dt.hour
        df["day_of_week"] = df["DateTime"].dt.dayofweek
        df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
        df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)

    # --- Fill NaNs from lag/rolling ---
    df = df.bfill().ffill().fillna(0)

    return df


print("\n[1] Engineering features...")
df_train_fe = engineer_features(df_train_raw, is_train=True)
df_test_fe = engineer_features(df_test_raw, is_train=False)

# Get feature columns (everything except DateTime and target)
drop_cols = ["DateTime", TARGET]
feature_cols = [c for c in df_train_fe.columns if c not in drop_cols]

X_train = df_train_fe[feature_cols].values
y_train = df_train_fe[TARGET].values
X_test = df_test_fe[feature_cols].values
y_test = df_test_fe[TARGET].values

print(f"    Features engineered: {len(feature_cols)} total features")
print(f"    Train: {X_train.shape}, Test: {X_test.shape}")


# ============================================================================
# 3. FILTER OUTLIERS (Sound Level < 0.1 = mill stops)
# ============================================================================
mask = df_train_fe["Sound level"] >= 0.1
X_train_clean = X_train[mask]
y_train_clean = y_train[mask]
removed = (~mask).sum()
if removed > 0:
    print(f"    Removed {removed} mill-stop outliers from training data")


# ============================================================================
# 4. SCALE FEATURES
# ============================================================================
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train_clean)
X_test_scaled = scaler.transform(X_test)


# ============================================================================
# 5. BASELINE (original MLR accuracy = 95.21%)
# ============================================================================
print("\n" + "=" * 70)
print("MODEL COMPARISON")
print("=" * 70)
print(f"{'Model':<35} {'R²':>8} {'MAE':>8} {'RMSE':>8} {'MAPE%':>8} {'Acc%':>8}")
print("-" * 70)


def evaluate(name, model, X_tr, y_tr, X_te, y_te):
    """Train, predict, and print metrics."""
    model.fit(X_tr, y_tr)
    y_pred = model.predict(X_te)
    r2 = r2_score(y_te, y_pred)
    mae = mean_absolute_error(y_te, y_pred)
    rmse = np.sqrt(mean_squared_error(y_te, y_pred))
    mape = mean_absolute_percentage_error(y_te, y_pred) * 100
    acc = (1 - mape / 100) * 100
    print(f"{name:<35} {r2:>8.4f} {mae:>8.4f} {rmse:>8.4f} {mape:>8.2f} {acc:>8.2f}")
    return y_pred, acc, r2


results = {}

# --- Tier 1: Linear Models ---
print("\n--- Tier 1: Linear Models (Production-Ready) ---")
_, acc, _ = evaluate("Ridge Regression", Ridge(alpha=1.0), X_train_scaled, y_train_clean, X_test_scaled, y_test)
results["Ridge"] = acc

_, acc, _ = evaluate("Lasso Regression", Lasso(alpha=0.01, max_iter=10000), X_train_scaled, y_train_clean, X_test_scaled, y_test)
results["Lasso"] = acc

_, acc, _ = evaluate("ElasticNet", ElasticNet(alpha=0.01, l1_ratio=0.5, max_iter=10000), X_train_scaled, y_train_clean, X_test_scaled, y_test)
results["ElasticNet"] = acc

# --- Tier 2: Tree-Based Models ---
print("\n--- Tier 2: Tree-Based Models (Higher Accuracy) ---")
_, acc, _ = evaluate("Random Forest", RandomForestRegressor(n_estimators=200, max_depth=10, random_state=42), X_train_clean, y_train_clean, X_test_scaled, y_test)
results["Random Forest"] = acc

_, acc, _ = evaluate("Gradient Boosting", GradientBoostingRegressor(n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42), X_train_scaled, y_train_clean, X_test_scaled, y_test)
results["Gradient Boosting"] = acc

if HAS_XGB:
    _, acc, _ = evaluate("XGBoost", XGBRegressor(n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42, verbosity=0), X_train_scaled, y_train_clean, X_test_scaled, y_test)
    results["XGBoost"] = acc

if HAS_LGBM:
    _, acc, _ = evaluate("LightGBM", LGBMRegressor(n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42, verbose=-1), X_train_scaled, y_train_clean, X_test_scaled, y_test)
    results["LightGBM"] = acc

# --- Tier 3: Other Models ---
print("\n--- Tier 3: Neural Network & SVR ---")
_, acc, _ = evaluate("SVR (RBF)", SVR(kernel="rbf", C=10, epsilon=0.1), X_train_scaled, y_train_clean, X_test_scaled, y_test)
results["SVR"] = acc

_, acc, _ = evaluate("ANN (MLP)", MLPRegressor(hidden_layer_sizes=(64, 32), max_iter=1000, random_state=42, early_stopping=True), X_train_scaled, y_train_clean, X_test_scaled, y_test)
results["ANN"] = acc


# ============================================================================
# 6. STACKING ENSEMBLE
# ============================================================================
print("\n--- Tier 4: Stacking Ensemble ---")
estimators = [
    ("ridge", Ridge(alpha=1.0)),
    ("gbr", GradientBoostingRegressor(n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42)),
    ("svr", SVR(kernel="rbf", C=10, epsilon=0.1)),
]
if HAS_XGB:
    estimators.append(("xgb", XGBRegressor(n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42, verbosity=0)))

stacker = StackingRegressor(
    estimators=estimators,
    final_estimator=Ridge(alpha=0.5),
    cv=5,
)
_, acc, _ = evaluate("Stacking Ensemble", stacker, X_train_scaled, y_train_clean, X_test_scaled, y_test)
results["Stacking Ensemble"] = acc


# ============================================================================
# 7. FEATURE IMPORTANCE (from best tree model)
# ============================================================================
print("\n" + "=" * 70)
print("TOP 15 MOST IMPORTANT FEATURES")
print("=" * 70)

gb_model = GradientBoostingRegressor(n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42)
gb_model.fit(X_train_scaled, y_train_clean)
importances = gb_model.feature_importances_
feat_imp = sorted(zip(feature_cols, importances), key=lambda x: x[1], reverse=True)

for i, (feat, imp) in enumerate(feat_imp[:15]):
    bar = "█" * int(imp * 200)
    print(f"  {i+1:2d}. {feat:<35} {imp:.4f} {bar}")


# ============================================================================
# 8. SUMMARY
# ============================================================================
print("\n" + "=" * 70)
print("SUMMARY — COMPARISON WITH ORIGINAL BASELINE")
print("=" * 70)
print(f"\n  Original MLR Accuracy (from Excel):  95.21%")
print(f"  Original RF Accuracy (from Excel):   94.07%")
print(f"  Original SVR Accuracy (from Excel):  94.63%")
print(f"  Original ANN Accuracy (from Excel):  94.55%")
print()

best_model = max(results, key=results.get)
best_acc = results[best_model]
improvement = best_acc - 95.21

print(f"  >>> BEST MODEL: {best_model} at {best_acc:.2f}% accuracy")
if improvement > 0:
    print(f"  >>> IMPROVEMENT over baseline: +{improvement:.2f}%")
else:
    print(f"  >>> vs baseline: {improvement:.2f}%")

print("\n  All model results:")
for name, acc in sorted(results.items(), key=lambda x: x[1], reverse=True):
    delta = acc - 95.21
    marker = " ✓ BEATS BASELINE" if delta > 0 else ""
    print(f"    {name:<30} {acc:>7.2f}%  ({delta:+.2f}%){marker}")

print("\n" + "=" * 70)
print("DONE")
print("=" * 70)
