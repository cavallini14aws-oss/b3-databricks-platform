from pipelines.examples.pdf_rag_lab.backend.services.retrieval_router import detect_intent


def test_detect_single_document_by_title():
    intent = detect_intent(
        "No PDF sobre o Livro Vermelho, qual é o tema principal?",
        {
            "o livro vermelho": ["o livro vermelho", "livro vermelho"],
            "os arquetipos e o inconsciente coletivo": [
                "os arquetipos e o inconsciente coletivo",
                "arquetipos inconsciente coletivo",
            ],
        },
    )
    assert intent.mode == "single_document"
    assert intent.document_hint == "o livro vermelho"


def test_detect_compare_documents():
    intent = detect_intent(
        "Compare os principais temas de O Livro Vermelho e Os Arquétipos e o Inconsciente Coletivo.",
        {
            "o livro vermelho": ["o livro vermelho", "livro vermelho"],
            "os arquetipos e o inconsciente coletivo": [
                "os arquetipos e o inconsciente coletivo",
                "arquetipos inconsciente coletivo",
            ],
        },
    )
    assert intent.mode == "compare_documents"
    assert intent.document_hints == [
        "o livro vermelho",
        "os arquetipos e o inconsciente coletivo",
    ]


def test_detect_broad_summary():
    intent = detect_intent(
        "Quais são os principais temas tratados nos PDFs carregados?",
        {
            "o livro vermelho": ["o livro vermelho", "livro vermelho"],
            "os arquetipos e o inconsciente coletivo": [
                "os arquetipos e o inconsciente coletivo",
                "arquetipos inconsciente coletivo",
            ],
        },
    )
    assert intent.mode == "broad_summary"
