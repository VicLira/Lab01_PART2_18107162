FROM python:3.12-slim

# Evita gerar arquivos .pyc
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Diretório de trabalho
WORKDIR /app

# Instala dependências do sistema
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instala UV corretamente (forma oficial)
RUN curl -Ls https://astral.sh/uv/install.sh | sh

# Garante que o uv está no PATH
ENV PATH="/root/.local/bin:$PATH"
    
# Instala UV
COPY pyproject.toml uv.lock ./

# Instala dependências
RUN uv sync --frozen

# Copia o restante do projeto
COPY . .

# Comando padrão
CMD ["uv", "run", "python", "src/main.py"]
