from typing import List

# IDS de produtos mapeados para Estética
ESTETICA_PRODUCT_IDS = {
    "5587176",
    "5554091",
    "5587203",
    "5560445",
    "5588268",
    "5716749",
    "6289449",
    "6289465",
}


def get_segment_for_products(product_ids: List[str]) -> str:
    """
    Categorizes a list of product IDs into a segment.
    - If all are in ESTETICA_PRODUCT_IDS -> 'ESTETICA'
    - If none are in ESTETICA_PRODUCT_IDS -> 'ILPI'
    - If there's a mix -> 'AMBOS'
    """
    if not product_ids:
        return None

    has_estetica = any(pid in ESTETICA_PRODUCT_IDS for pid in product_ids)
    has_ilpi = any(pid not in ESTETICA_PRODUCT_IDS for pid in product_ids)

    if has_estetica and has_ilpi:
        return "AMBOS"
    if has_estetica:
        return "ESTETICA"
    return "ILPI"
