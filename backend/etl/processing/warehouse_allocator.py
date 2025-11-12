#warehouse_allocator.py
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist

class WarehouseAllocator:
    """
    Asigna ubicaciones óptimas de warehouse usando clustering geográfico (K-Means)
    basado en la densidad de pedidos y coordenadas de clientes.
    """

    def __init__(self, df_orders, df_customers, df_geolocation, df_items, df_products, n_clusters=None):
        self.df_orders = df_orders
        self.df_customers = df_customers
        self.df_geolocation = df_geolocation
        self.df_items = df_items
        self.df_products = df_products
        self.n_clusters = n_clusters
        self.logs = []

    def estimate(self):
        print("Estimando ubicaciones óptimas de warehouse mediante clustering geográfico...")

        # Preparar data
        df_cust = self.df_customers.copy()
        df_geo = self.df_geolocation.copy()

        if "customer_zip_code_prefix" not in df_cust.columns:
            zip_col = [c for c in df_cust.columns if "zip" in c][0]
            df_cust = df_cust.rename(columns={zip_col: "customer_zip_code_prefix"})

        if "geolocation_zip_code_prefix" not in df_geo.columns:
            zip_col = [c for c in df_geo.columns if "zip" in c][0]
            df_geo = df_geo.rename(columns={zip_col: "geolocation_zip_code_prefix"})

        df_merge = pd.merge(
            df_cust,
            df_geo,
            left_on="customer_zip_code_prefix",
            right_on="geolocation_zip_code_prefix",
            how="left"
        )

        df_merge["geolocation_lat"] = pd.to_numeric(df_merge["geolocation_lat"], errors="coerce")
        df_merge["geolocation_lng"] = pd.to_numeric(df_merge["geolocation_lng"], errors="coerce")
        df_merge = df_merge.dropna(subset=["geolocation_lat", "geolocation_lng"])

        if df_merge.empty:
            raise ValueError("No hay coordenadas válidas para clientes tras el merge geográfico.")

        n_points = len(df_merge)
        print(f"Coordenadas válidas para clustering: {n_points} registros")

        coords = df_merge[["geolocation_lat", "geolocation_lng"]].values

        # Ajuste automático de n_clusters
        if self.n_clusters is None:
            self.n_clusters = int(max(30, min(120, np.sqrt(n_points) // 15)))
        if len(coords) < self.n_clusters:
            self.n_clusters = max(5, len(coords) // 2)

        print(f"Número de clusters ajustado automáticamente a: {self.n_clusters}")

        kmeans = KMeans(n_clusters=self.n_clusters, random_state=42, n_init=10)
        df_merge["cluster"] = kmeans.fit_predict(coords)

        # Vincular pedidos y productos
        df_full = (
            self.df_orders.merge(self.df_items, on="order_id", how="inner")
            .merge(self.df_products, on="product_id", how="left")
            .merge(df_merge[["customer_id", "cluster"]], on="customer_id", how="left")
        )

        if "cluster" not in df_full.columns or df_full["cluster"].isna().all():
            raise ValueError("No se pudo asignar ningún cluster a los clientes.")

        warehouses = []
        valid_clusters = sorted(df_full["cluster"].dropna().unique())
        total_clusters = len(valid_clusters)
        total_customers = df_merge["customer_id"].nunique()

        print(f"Iniciando análisis de {total_clusters} clusters...\n")

        for idx, cluster_id in enumerate(valid_clusters, start=1):
            cluster_points = df_merge[df_merge["cluster"] == cluster_id]
            if cluster_points.empty:
                continue

            # Detección de outliers por distancia al centroide
            lat_mean = cluster_points["geolocation_lat"].mean()
            lon_mean = cluster_points["geolocation_lng"].mean()
            coords_cluster = cluster_points[["geolocation_lat", "geolocation_lng"]].values
            centroid = np.array([lat_mean, lon_mean])
            distances = cdist(coords_cluster, [centroid])
            outlier_mask = distances[:,0] > np.percentile(distances, 95)
            cluster_points_filtered = cluster_points[~outlier_mask]

            # Densidad y clientes únicos
            density = cluster_points_filtered["customer_id"].nunique()
            relative_density = density / total_customers

            cluster_items = df_full[df_full["cluster"] == cluster_id]
            top_items = cluster_items["product_id"].value_counts().head(5).index.tolist()

            note = None

            # Subdivisión adaptativa
            if relative_density > 0.08:
                sub_k = min(3, int(relative_density * 100))
                sub_kmeans = KMeans(n_clusters=sub_k, random_state=42, n_init=10)
                sub_labels = sub_kmeans.fit_predict(cluster_points_filtered[["geolocation_lat", "geolocation_lng"]].values)

                for sub_id in range(sub_k):
                    sub_points = cluster_points_filtered[sub_labels == sub_id]
                    if sub_points.empty:
                        continue

                    lat_sub = sub_points["geolocation_lat"].mean()
                    lon_sub = sub_points["geolocation_lng"].mean()
                    sub_density = sub_points["customer_id"].nunique()
                    sub_ratio = sub_density / total_customers

                    # Tamaño adaptativo
                    if sub_ratio > 0.04:
                        size = "large"
                    elif sub_ratio > 0.015:
                        size = "medium"
                    else:
                        size = "small"

                    # Mejora de delivery ponderada por densidad
                    base_improv = 10
                    max_improv = 25
                    density_factor = min(sub_ratio / 0.1, 1)
                    improvement = round(base_improv + (max_improv - base_improv) * density_factor, 2)

                    warehouses.append({
                        "warehouse_id": f"{cluster_id}_{sub_id}",
                        "latitude": float(lat_sub),
                        "longitude": float(lon_sub),
                        "customer_count": int(sub_density),
                        "density_ratio": round(sub_ratio, 4),
                        "warehouse_size": size,
                        "estimated_delivery_improvement_%": improvement,
                        "top_items": top_items,
                        "note": "Subdividido por alta densidad de clientes en área metropolitana"
                    })

                self.logs.append({
                    "cluster_id": cluster_id,
                    "total_customers": density,
                    "lat_mean": float(lat_mean),
                    "lon_mean": float(lon_mean),
                    "density_ratio": round(relative_density,4),
                    "subclusters": sub_k,
                    "outliers_removed": int(outlier_mask.sum())
                })
                continue

            # Clasificación normal
            if relative_density > 0.04:
                size = "large"
            elif relative_density > 0.015:
                size = "medium"
            else:
                size = "small"

            base_improv = 10
            max_improv = 25
            density_factor = min(relative_density / 0.1, 1)
            improvement = round(base_improv + (max_improv - base_improv) * density_factor, 2)

            warehouses.append({
                "warehouse_id": int(cluster_id + 1),
                "latitude": float(lat_mean),
                "longitude": float(lon_mean),
                "customer_count": int(density),
                "density_ratio": round(relative_density, 4),
                "warehouse_size": size,
                "estimated_delivery_improvement_%": improvement,
                "top_items": top_items,
                "note": note or "Cluster normal sin subdivisión"
            })

            self.logs.append({
                "cluster_id": cluster_id,
                "total_customers": density,
                "lat_mean": float(lat_mean),
                "lon_mean": float(lon_mean),
                "density_ratio": round(relative_density,4),
                "subclusters": 0,
                "outliers_removed": int(outlier_mask.sum())
            })

        sizes = [w["warehouse_size"] for w in warehouses]
        print(
            f"\n{len(warehouses)} ubicaciones estimadas | "
            f"Distribución: {sizes.count('small')} small | "
            f"{sizes.count('medium')} medium | "
            f"{sizes.count('large')} large"
        )

        return warehouses
