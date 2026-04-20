from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, func
from database import Base


class Inventario(Base):
    __tablename__ = "inventarios"

    id = Column(String, primary_key=True, index=True)
    nome = Column(String, nullable=False)
    senha = Column(String, nullable=False)
    status = Column(String, nullable=False, default="ABERTO")


class Grupo(Base):
    __tablename__ = "grupos"

    id = Column(String, primary_key=True, index=True)
    id_inventario = Column(String, ForeignKey("inventarios.id"), nullable=False)
    nome = Column(String, nullable=False)
    meta = Column(Integer, nullable=False, default=0)
    status = Column(String, nullable=False, default="DISPONIVEL")
    colaborativo = Column(Boolean, nullable=False, default=False)
    vagas = Column(Integer, nullable=False, default=1)


class UsuarioAtivo(Base):
    __tablename__ = "usuarios_ativos"

    id = Column(Integer, primary_key=True, autoincrement=True)
    usuario = Column(String, nullable=False)
    id_inventario = Column(String, ForeignKey("inventarios.id"), nullable=False)
    id_grupo = Column(String, ForeignKey("grupos.id"), nullable=False)
    grupo_nome = Column(String, nullable=False)
    entrou_em = Column(DateTime(timezone=True), server_default=func.now())


class Bipe(Base):
    """
    Agora armazena bipes agrupados por (usuario, id_inventario, id_grupo, ean).
    'quantidade' representa quantas vezes aquele EAN foi bipado.
    'criado_em' é o primeiro bipe; 'atualizado_em' é o último.
    O painel continua exibindo item por item — cada linha representa 1 unidade
    expandida via quantidade, sem mudar a experiência visual.
    """
    __tablename__ = "bipes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    usuario = Column(String, nullable=False)
    id_inventario = Column(String, ForeignKey("inventarios.id"), nullable=False)
    id_grupo = Column(String, ForeignKey("grupos.id"), nullable=False)
    grupo_nome = Column(String, nullable=False)
    ean = Column(String, nullable=False)
    quantidade = Column(Integer, nullable=False, default=1)
    criado_em = Column(DateTime(timezone=True), server_default=func.now())
    atualizado_em = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Estoque(Base):
    """
    Tabela de estoque sem as colunas filial e grade,
    que foram removidas por não serem necessárias.
    """
    __tablename__ = "estoque"

    id = Column(Integer, primary_key=True, autoincrement=True)
    produto = Column(String, nullable=False)
    cor_produ = Column(String, nullable=False)
    tamanho = Column(String, nullable=True)
    quantidade = Column(Integer, nullable=False, default=0)
    ean = Column(String, nullable=False, unique=True, index=True)
    ref_cor = Column(String, nullable=True)
    ativo = Column(Boolean, nullable=False, default=True)
    importado_em = Column(DateTime(timezone=True), server_default=func.now())
