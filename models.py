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
    __tablename__ = "bipes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    usuario = Column(String, nullable=False)
    id_inventario = Column(String, ForeignKey("inventarios.id"), nullable=False)
    id_grupo = Column(String, ForeignKey("grupos.id"), nullable=False)
    grupo_nome = Column(String, nullable=False)
    ean = Column(String, nullable=False)
    criado_em = Column(DateTime(timezone=True), server_default=func.now())


class Estoque(Base):
    __tablename__ = "estoque"

    id = Column(Integer, primary_key=True, autoincrement=True)
    produto = Column(String, nullable=False)
    cor_produ = Column(String, nullable=False)
    filial = Column(String, nullable=True)
    tamanho = Column(String, nullable=True)
    quantidade = Column(Integer, nullable=False, default=0)
    grade = Column(String, nullable=True)
    ean = Column(String, nullable=False, unique=True, index=True)
    ref_cor = Column(String, nullable=True)
    ativo = Column(Boolean, nullable=False, default=True)
    importado_em = Column(DateTime(timezone=True), server_default=func.now())