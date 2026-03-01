import csv
import os
from src.config import Config


def generate_manychat_sample():
    """Generates a sample ManyChat CSV file in the input directory."""
    target_dir = Config.MANYCHAT_INPUT_DIR
    os.makedirs(target_dir, exist_ok=True)

    file_path = os.path.join(target_dir, "manychat_sample_anonymized.csv")

    headers = [
        "nome",
        "email",
        "instagram",
        "whatsapp",
        "data_remarketing",
        "agendamento",
        "data_agendamento",
        "contactar",
        "data_contactar",
        "ultima_interacao",
        "data_registro",
    ]

    data = [
        {
            "nome": "João Silva (Teste)",
            "email": "joao.teste@example.com",
            "instagram": "joao_insta",
            "whatsapp": "5511988887777",
            "data_remarketing": "46057,56185",
            "agendamento": "NAO",
            "data_agendamento": "",
            "contactar": "SIM",
            "data_contactar": "46058,56185",
            "ultima_interacao": "46057,0",
            "data_registro": "46000,0",
        },
        {
            "nome": "Maria Santos (Teste)",
            "email": "maria.teste@example.com",
            "instagram": "maria_style",
            "whatsapp": "5511977776666",
            "data_remarketing": "",
            "agendamento": "SIM",
            "data_agendamento": "46060,0",
            "contactar": "NAO",
            "data_contactar": "",
            "ultima_interacao": "46059,0",
            "data_registro": "46050,0",
        },
        {
            "nome": "Ghost User (No Phone)",
            "email": "ghost@example.com",
            "instagram": "ghost_insta",
            "whatsapp": "",
            "data_remarketing": "",
            "agendamento": "",
            "data_agendamento": "",
            "contactar": "",
            "data_contactar": "",
            "ultima_interacao": "",
            "data_registro": "",
        },
    ]

    with open(file_path, mode="w", encoding="utf-8") as f:
        # ManyChat format uses tab delimiter
        writer = csv.DictWriter(f, fieldnames=headers, delimiter="\t")
        writer.writeheader()
        writer.writerows(data)

    print(f"Sample data generated at: {file_path}")


if __name__ == "__main__":
    generate_manychat_sample()
