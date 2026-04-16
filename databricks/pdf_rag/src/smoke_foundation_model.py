from __future__ import annotations

from src.foundation_model_client import invoke_foundation_model


def main() -> None:
    prompt = "Responda apenas OK."
    result = invoke_foundation_model(
        prompt=prompt,
        system_prompt="Você é um verificador técnico. Responda de forma mínima.",
        max_tokens=16,
        temperature=0.0,
    )
    print(result)


if __name__ == "__main__":
    main()
