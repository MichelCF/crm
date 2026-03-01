import csv
import re
import argparse
from pathlib import Path
from collections import defaultdict

DDD_TO_STATE = {
    "11": "SP",
    "12": "SP",
    "13": "SP",
    "14": "SP",
    "15": "SP",
    "16": "SP",
    "17": "SP",
    "18": "SP",
    "19": "SP",
    "21": "RJ",
    "22": "RJ",
    "24": "RJ",
    "27": "ES",
    "28": "ES",
    "31": "MG",
    "32": "MG",
    "33": "MG",
    "34": "MG",
    "35": "MG",
    "37": "MG",
    "38": "MG",
    "41": "PR",
    "42": "PR",
    "43": "PR",
    "44": "PR",
    "45": "PR",
    "46": "PR",
    "47": "SC",
    "48": "SC",
    "49": "SC",
    "51": "RS",
    "53": "RS",
    "54": "RS",
    "55": "RS",
    "61": "DF",
    "62": "GO",
    "64": "GO",
    "63": "TO",
    "65": "MT",
    "66": "MT",
    "67": "MS",
    "68": "AC",
    "69": "RO",
    "71": "BA",
    "73": "BA",
    "74": "BA",
    "75": "BA",
    "77": "BA",
    "79": "SE",
    "81": "PE",
    "87": "PE",
    "82": "AL",
    "83": "PB",
    "84": "RN",
    "85": "CE",
    "88": "CE",
    "86": "PI",
    "89": "PI",
    "91": "PA",
    "93": "PA",
    "94": "PA",
    "92": "AM",
    "97": "AM",
    "95": "RR",
    "96": "AP",
    "98": "MA",
    "99": "MA",
}


def normalize_phone_and_get_state(raw_phone: str):
    """
    Cleans up the phone, ensures it starts with 55 (BR),
    extracts the DDD and maps it to the respective State (UF).
    Returns a tuple: (formatted_phone, state_uf)
    """
    if not raw_phone:
        return "", ""

    # Remove everything that is not a digit
    digits = re.sub(r"\D", "", raw_phone)
    if not digits:
        return "", ""

    # If the phone is something like "11999999999" (10 or 11 digits, no country code)
    if len(digits) in (10, 11) and not digits.startswith("55"):
        digits = "55" + digits

    # If it is missing a 9 maybe (e.g. 1188888888 -> 551188888888)
    elif len(digits) == 12 and digits.startswith("55"):
        pass
    elif len(digits) == 13 and digits.startswith("55"):
        pass
    else:
        # Default fallback, just prepend 55 if not there
        if not digits.startswith("55"):
            digits = "55" + digits

    # Try to extract DDD assuming 55 + DDD + Number
    ddd = ""
    state = ""
    if len(digits) >= 12 and digits.startswith("55"):
        ddd = digits[2:4]
        state = DDD_TO_STATE.get(ddd, "")

    return digits, state


def parse_monetary_value(val_str: str) -> float:
    if not val_str:
        return 0.0
    # Replace comma with dot if present to parse nicely
    cleaned = val_str.replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def export_meta_audience(product_ids: list[str], output_file: str):
    hotmart_dir = Path("data/hotmart")

    # Store aggregated info keyed by email
    # customers[email] = { "name": ..., "phone_raw": ..., "ltv": 0.0, "interacted": False }
    customers = defaultdict(
        lambda: {"name": "", "phone_raw": "", "ltv": 0.0, "interacted": False}
    )

    if not hotmart_dir.exists():
        print(f"Erro: diretório {hotmart_dir} não encontrado.")
        return

    csv_files = list(hotmart_dir.glob("*.csv"))
    if not csv_files:
        print(f"Nenhum arquivo CSV encontrado em {hotmart_dir}.")
        return

    print(f"Lendo {len(csv_files)} arquivos CSV e agrupando por e-mail...")
    for csv_file in csv_files:
        with open(csv_file, "r", encoding="utf-8-sig", errors="replace") as f:
            reader = csv.DictReader(f, delimiter=";")  # Hotmart uses semicolons

            for row in reader:
                email = row.get("Email", "").strip().lower()
                if not email:
                    continue

                prod_id = row.get("Código do Produto", "").strip()
                status = row.get("Status", "").strip().lower()
                name = row.get("Nome", "").strip()

                # Fetch phone fields (usually 'DDD' and 'Telefone')
                ddd = row.get("DDD", "").strip()
                telefone = row.get("Telefone", "").strip()
                full_phone = ddd + telefone if ddd and telefone else telefone

                # Preço da Oferta or Preço Total? Taking Preço Total
                valor_pago = parse_monetary_value(row.get("Preço Total", ""))

                # Check interaction with target product segment
                if prod_id in product_ids:
                    customers[email]["interacted"] = True

                # Always grab Name and Phone if missing
                if name and not customers[email]["name"]:
                    customers[email]["name"] = name
                if full_phone and not customers[email]["phone_raw"]:
                    customers[email]["phone_raw"] = full_phone

                # Add to LTV if Approved or Complete
                if status in ("completo", "aprovado"):
                    customers[email]["ltv"] += valor_pago

    headers = ["name", "email", "phone", "country", "state", "value"]

    exported_count = 0
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for email, data in customers.items():
            # Apply business rule: We only want them if they interacted with target products
            if not data["interacted"]:
                continue

            name = data["name"]
            raw_phone = data["phone_raw"]
            value = round(data["ltv"], 2)

            phone, state = normalize_phone_and_get_state(raw_phone)
            country = "BR"

            writer.writerow([name, email, phone, country, state, value])
            exported_count += 1

    print(f"Exportação concluída! {exported_count} contatos salvos em '{output_file}'.")


def get_estetica_product_ids() -> list[str]:
    """
    Retorna a lista de IDs de produtos do segmento de estética.
    """
    return [
        "5587176",
        "5554091",
        "5587203",
        "5560445",
        "5588268",
        "5716749",
        "6289449",
        "6289465",
    ]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Gera uma lista de público para a Meta Ads lendo CSVs da Hotmart."
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/meta_audience_export.csv",
        help="Caminho do CSV de saída",
    )

    args = parser.parse_args()

    # Obtém os IDs de produtos mapeados
    product_ids = get_estetica_product_ids()
    export_meta_audience(product_ids, args.output)
