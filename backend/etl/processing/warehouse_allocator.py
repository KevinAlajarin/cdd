import pandas as pd
import numpy as np
from sklearn.cluster import KMeans


class WarehouseAllocator:
    """
    Asigna ubicaciones óptimas de warehouse usando clustering geográfico (K-Means)
    basado en la densidad de pedidos y coordenadas de clientes.
    """

    def __init__(self, df_orders, df_customers, df_geolocation, df_items, df_products, n_clusters=60):
        """
        Parámetros:
            df_orders: pedidos filtrados (solo entregados)
            df_customers: clientes con sus códigos postales
            df_geolocation: coordenadas lat/lon por zip code
            df_items: ítems de pedido
            df_products: catálogo de productos
            n_clusters: número de clusters (warehouses a estimar)
        """
        self.df_orders = df_orders
        self.df_customers = df_customers
        self.df_geolocation = df_geolocation
        self.df_items = df_items
        self.df_products = df_products
        self.n_clusters = n_clusters

    # ============================================================
    # MÉTODO PRINCIPAL
    # ============================================================
    def estimate(self):
        print("Estimando ubicaciones óptimas de warehouse mediante clustering geográfico...")

        # === 1️⃣ Preparar data ===
        df_cust = self.df_customers.copy()
        df_geo = self.df_geolocation.copy()

        # Renombrar columnas si es necesario
        if "customer_zip_code_prefix" not in df_cust.columns:
            zip_col = [c for c in df_cust.columns if "zip" in c][0]
            df_cust = df_cust.rename(columns={zip_col: "customer_zip_code_prefix"})

        if "geolocation_zip_code_prefix" not in df_geo.columns:
            zip_col = [c for c in df_geo.columns if "zip" in c][0]
            df_geo = df_geo.rename(columns={zip_col: "geolocation_zip_code_prefix"})

        # === 2️⃣ Merge clientes + coordenadas ===
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

        print(f"Coordenadas válidas para clustering: {len(df_merge)} registros")

        # === 3️⃣ Clustering geográfico ===
        coords = df_merge[["geolocation_lat", "geolocation_lng"]].values

        # 🔧 CORRECCIÓN: asegurar que self.n_clusters siempre sea válido
        if self.n_clusters is None or not isinstance(self.n_clusters, int) or self.n_clusters <= 0:
            self.n_clusters = 60

        # Ajustar si hay pocos puntos válidos
        if len(coords) < self.n_clusters:
            self.n_clusters = max(5, len(coords) // 2)

        # Si aun así el número es inválido, forzar a 5 como mínimo
        if self.n_clusters is None or self.n_clusters < 1:
            self.n_clusters = 5

        from sklearn.cluster import KMeans
        kmeans = KMeans(n_clusters=self.n_clusters, random_state=42, n_init=10)
        df_merge["cluster"] = kmeans.fit_predict(coords)

        # === 4️⃣ Vincular pedidos y productos a cada cluster ===
        df_orders = self.df_orders.copy()
        df_items = self.df_items.copy()
        df_products = self.df_products.copy()

        df_full = (
            df_orders.merge(df_items, on="order_id", how="inner")
            .merge(df_products, on="product_id", how="left")
            .merge(df_merge[["customer_id", "cluster"]], on="customer_id", how="left")
        )

        if "cluster" not in df_full.columns or df_full["cluster"].isna().all():
            raise ValueError("No se pudo asignar ningún cluster a los clientes. Verifica coordenadas y zip codes.")

        # === 5️⃣ Calcular resumen por warehouse ===
        warehouses = []
        valid_clusters = df_full["cluster"].dropna().unique()

        for cluster_id in sorted(valid_clusters):
            cluster_points = df_merge[df_merge["cluster"] == cluster_id]
            if cluster_points.empty:
                continue

            lat_mean = cluster_points["geolocation_lat"].mean()
            lon_mean = cluster_points["geolocation_lng"].mean()

            cluster_items = df_full[df_full["cluster"] == cluster_id]
            top_items = (
                cluster_items["product_id"]
                .value_counts()
                .head(5)
                .index.tolist()
            )

            density = len(cluster_points)
            relative_density = density / df_merge["customer_id"].nunique()

            if relative_density > 0.05:
                size = "large"
            elif relative_density > 0.02:
                size = "medium"
            else:
                size = "small"

            improvement = round(min(30, 5 + relative_density * 100), 2)

            warehouses.append({
                "warehouse_id": int(cluster_id + 1),
                "latitude": float(lat_mean),
                "longitude": float(lon_mean),
                "customer_count": int(len(cluster_points)),
                "density_ratio": round(relative_density, 4),
                "warehouse_size": size,
                "estimated_delivery_improvement_%": improvement,
                "top_items": top_items,
            })

        print(f"{len(warehouses)} ubicaciones de warehouse estimadas con productos top y mejora estimada.")
        return warehouses

