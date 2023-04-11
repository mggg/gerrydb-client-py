"""Repository for districting plans."""
from typing import Optional, Union

from cherrydb.repos.base import ObjectRepo, err, namespaced, online, write_context
from cherrydb.schemas import Geography, GeoLayer, Locality, Plan, PlanCreate


class PlanRepo(ObjectRepo[Plan]):
    """Repository for districting plans."""

    @err("Failed to create districting plan")
    @namespaced
    @write_context
    @online
    def create(
        self,
        path: str,
        namespace: Optional[str] = None,
        *,
        locality: str | Locality,
        layer: str | GeoLayer,
        assignments: dict[Union[Geography, str], int],
        description: str,
        source_url: str | None = None,
        districtr_id: str | None = None,
        daves_id: str | None = None,
    ) -> Plan:
        """Creates a districting plan.

        Args:
            path: A short identifier for the plan (e.g. `block_groups`).
            namespace: The plan's namespace.
            locality: `Locality` (or locality path) to associate the plan with.
            layer: `GeoLayer` (or layer path) to associate the plan with.
            description: Longform description of the plan.
            source_url: Original source of the plan (e.g. a Districtr permalink).
            districtr_id: Districtr identifier for the plan (optional).
            daves_id: Dave's Redistricting identifier for the plan (optional).

        Raises:
            RequestError: If the plan cannot be created on the server side,
                or if the parameters fail validation.

        Returns:
            The new districting plan.
        """
        response = self.ctx.client.post(
            f"{self.base_url}/{namespace}",
            json=PlanCreate(
                path=path,
                description=description,
                source_url=source_url,
                districtr_id=districtr_id,
                daves_id=daves_id,
                locality=(
                    locality.canonical_path
                    if isinstance(locality, Locality)
                    else locality
                ),
                layer=layer.full_path if isinstance(layer, GeoLayer) else layer,
                assignments={
                    geo.full_path if isinstance(geo, Geography) else geo: assignment
                    for geo, assignment in assignments.items()
                },
            ).dict(),
        )
        response.raise_for_status()

        return self.schema(**response.json())
