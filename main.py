from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import func
from database import Base, engine, SessionLocal
from models import Inventario, Grupo, UsuarioAtivo, Bipe, Estoque
from openpyxl import load_workbook
import csv
import io
import re

app = FastAPI(title="HS Inventário API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

Base.metadata.create_all(bind=engine)


class InventarioIn(BaseModel):
    id: str
    nome: str
    senha: str


class GrupoIn(BaseModel):
    id: str
    id_inventario: str
    nome: str
    meta: int
    colaborativo: bool = False
    vagas: int = 1


class EntrarGrupoIn(BaseModel):
    usuario: str
    id_inventario: str
    id_grupo: str


class BipeIn(BaseModel):
    usuario: str
    id_inventario: str
    id_grupo: str
    ean: str


class ConcluirGrupoIn(BaseModel):
    usuario: str
    id_inventario: str
    id_grupo: str


class ResetarGrupoIn(BaseModel):
    usuario: str
    id_inventario: str
    id_grupo: str


class EditarMetaIn(BaseModel):
    id_inventario: str
    id_grupo: str
    nova_meta: int


class TornarColaborativoIn(BaseModel):
    id_inventario: str
    id_grupo: str
    vagas: int = 2


class RenomearGrupoIn(BaseModel):
    id_inventario: str
    id_grupo: str
    novo_nome: str


class RemoverDoGrupoIn(BaseModel):
    usuario: str
    id_inventario: str


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def normalizar_ean(valor):
    s = str(valor or "").strip()
    s = re.sub(r"\.0+$", "", s)
    s = re.sub(r"[^0-9]", "", s)
    return s


def norm_txt(valor):
    return str(valor or "").strip()


def montar_ref_cor(produto, cor):
    return f"{norm_txt(produto)}{norm_txt(cor)}".strip()


def obter_grupo_ativo_do_usuario(db: Session, usuario: str, id_inventario: str):
    return db.query(UsuarioAtivo).filter(
        UsuarioAtivo.usuario == usuario,
        UsuarioAtivo.id_inventario == id_inventario
    ).first()


def contar_bipes_grupo(db: Session, id_inventario: str, id_grupo: str) -> int:
    return db.query(func.count(Bipe.id)).filter(
        Bipe.id_inventario == id_inventario,
        Bipe.id_grupo == id_grupo
    ).scalar() or 0


def listar_membros_grupo(db: Session, id_inventario: str, id_grupo: str):
    return db.query(UsuarioAtivo).filter(
        UsuarioAtivo.id_inventario == id_inventario,
        UsuarioAtivo.id_grupo == id_grupo
    ).all()


@app.get("/")
def home():
    return {"status": "API rodando com banco"}


@app.post("/inventarios")
def criar_inventario(data: InventarioIn, db: Session = Depends(get_db)):
    existe = db.get(Inventario, data.id)
    if existe:
        raise HTTPException(status_code=400, detail="Inventário já existe")

    inv = Inventario(
        id=data.id,
        nome=data.nome,
        senha=data.senha,
        status="ABERTO"
    )
    db.add(inv)
    db.commit()
    return {"success": True}


@app.get("/inventarios")
def listar_inventarios(db: Session = Depends(get_db)):
    itens = db.query(Inventario).all()
    return {
        "success": True,
        "inventarios": [
            {
                "id": i.id,
                "nome": i.nome,
                "senha": i.senha,
                "status": i.status
            } for i in itens
        ]
    }


@app.post("/grupos")
def criar_grupo(data: GrupoIn, db: Session = Depends(get_db)):
    inv = db.get(Inventario, data.id_inventario)
    if not inv:
        raise HTTPException(status_code=404, detail="Inventário não encontrado")

    existe = db.get(Grupo, data.id)
    if existe:
        raise HTTPException(status_code=400, detail="Grupo já existe")

    grupo = Grupo(
        id=data.id,
        id_inventario=data.id_inventario,
        nome=data.nome,
        meta=data.meta,
        status="DISPONIVEL",
        colaborativo=data.colaborativo,
        vagas=data.vagas
    )
    db.add(grupo)
    db.commit()
    return {"success": True}


@app.get("/grupos/{id_inventario}")
def listar_grupos(id_inventario: str, db: Session = Depends(get_db)):
    lista = db.query(Grupo).filter(Grupo.id_inventario == id_inventario).all()
    grupos = []
    for g in lista:
        membros = db.query(UsuarioAtivo).filter(
            UsuarioAtivo.id_inventario == g.id_inventario,
            UsuarioAtivo.id_grupo == g.id
        ).all()
        grupos.append({
            "id": g.id,
            "id_inventario": g.id_inventario,
            "nome": g.nome,
            "meta": g.meta,
            "status": g.status,
            "colaborativo": g.colaborativo,
            "vagas": g.vagas,
            "membros": [m.usuario for m in membros]
        })
    return {"success": True, "grupos": grupos}


@app.post("/grupos/entrar")
def entrar_grupo(data: EntrarGrupoIn, db: Session = Depends(get_db)):
    grupo = db.query(Grupo).filter(
        Grupo.id == data.id_grupo,
        Grupo.id_inventario == data.id_inventario
    ).first()

    if not grupo:
        raise HTTPException(status_code=404, detail="Grupo não encontrado")

    if grupo.status == "CONCLUIDO":
        raise HTTPException(status_code=400, detail="Grupo concluído")

    membros = db.query(UsuarioAtivo).filter(
        UsuarioAtivo.id_inventario == data.id_inventario,
        UsuarioAtivo.id_grupo == data.id_grupo
    ).all()

    ja_ativo = db.query(UsuarioAtivo).filter(
        UsuarioAtivo.usuario == data.usuario,
        UsuarioAtivo.id_inventario == data.id_inventario
    ).first()

    if ja_ativo:
        ja_ativo.id_grupo = grupo.id
        ja_ativo.grupo_nome = grupo.nome
    else:
        if not grupo.colaborativo and len(membros) >= 1:
            raise HTTPException(status_code=400, detail="Grupo já reservado")
        if grupo.colaborativo and len(membros) >= grupo.vagas:
            raise HTTPException(status_code=400, detail="Sem vagas")

        novo = UsuarioAtivo(
            usuario=data.usuario,
            id_inventario=data.id_inventario,
            id_grupo=grupo.id,
            grupo_nome=grupo.nome
        )
        db.add(novo)

    grupo.status = "RESERVADO"
    db.commit()

    membros_atualizados = db.query(UsuarioAtivo).filter(
        UsuarioAtivo.id_inventario == data.id_inventario,
        UsuarioAtivo.id_grupo == data.id_grupo
    ).all()

    return {
        "success": True,
        "grupo": grupo.nome,
        "meta": grupo.meta,
        "colaborativo": grupo.colaborativo,
        "membros": [m.usuario for m in membros_atualizados]
    }


@app.get("/usuario/ativo")
def usuario_ativo(usuario: str, id_inventario: str, db: Session = Depends(get_db)):
    ativo = db.query(UsuarioAtivo).filter(
        UsuarioAtivo.usuario == usuario,
        UsuarioAtivo.id_inventario == id_inventario
    ).first()

    if not ativo:
        return {"ativo": False}

    grupo = db.query(Grupo).filter(
        Grupo.id == ativo.id_grupo,
        Grupo.id_inventario == id_inventario
    ).first()

    if not grupo:
        return {"ativo": False}

    total = db.query(func.count(Bipe.id)).filter(
        Bipe.id_inventario == id_inventario,
        Bipe.id_grupo == ativo.id_grupo
    ).scalar() or 0

    membros = db.query(UsuarioAtivo).filter(
        UsuarioAtivo.id_inventario == id_inventario,
        UsuarioAtivo.id_grupo == ativo.id_grupo
    ).all()

    return {
        "ativo": True,
        "usuario": usuario,
        "id_grupo": ativo.id_grupo,
        "grupo_nome": ativo.grupo_nome,
        "meta": grupo.meta,
        "colaborativo": grupo.colaborativo,
        "bipes": total,
        "membros": [m.usuario for m in membros]
    }


@app.post("/bipes")
def registrar_bipe(data: BipeIn, db: Session = Depends(get_db)):
    grupo = db.query(Grupo).filter(
        Grupo.id == data.id_grupo,
        Grupo.id_inventario == data.id_inventario
    ).first()

    if not grupo:
        raise HTTPException(status_code=404, detail="Grupo não encontrado")

    if grupo.status == "CONCLUIDO":
        raise HTTPException(status_code=400, detail="Grupo concluído")

    usuario_ok = db.query(UsuarioAtivo).filter(
        UsuarioAtivo.usuario == data.usuario,
        UsuarioAtivo.id_inventario == data.id_inventario,
        UsuarioAtivo.id_grupo == data.id_grupo
    ).first()

    if not usuario_ok:
        raise HTTPException(status_code=400, detail="Usuário não está ativo nesse grupo")

    registro = Bipe(
        usuario=data.usuario,
        id_inventario=data.id_inventario,
        id_grupo=data.id_grupo,
        grupo_nome=usuario_ok.grupo_nome,
        ean=data.ean
    )
    db.add(registro)
    db.commit()

    total_grupo = db.query(Bipe).filter(
        Bipe.id_inventario == data.id_inventario,
        Bipe.id_grupo == data.id_grupo
    ).count()

    return {"success": True, "total_grupo": total_grupo}



@app.post("/grupos/concluir")
def concluir_grupo(data: ConcluirGrupoIn, db: Session = Depends(get_db)):
    grupo = db.query(Grupo).filter(
        Grupo.id == data.id_grupo,
        Grupo.id_inventario == data.id_inventario
    ).first()

    if not grupo:
        raise HTTPException(status_code=404, detail="Grupo não encontrado")

    usuario_ativo = db.query(UsuarioAtivo).filter(
        UsuarioAtivo.usuario == data.usuario,
        UsuarioAtivo.id_inventario == data.id_inventario,
        UsuarioAtivo.id_grupo == data.id_grupo
    ).first()

    if not usuario_ativo:
        raise HTTPException(status_code=400, detail="Usuário não está ativo nesse grupo")

    total_grupo = db.query(func.count(Bipe.id)).filter(
        Bipe.id_inventario == data.id_inventario,
        Bipe.id_grupo == data.id_grupo
    ).scalar() or 0

    if int(total_grupo) != int(grupo.meta or 0):
        raise HTTPException(status_code=400, detail="CONTAGEM_NAO_BATE")

    membros = db.query(UsuarioAtivo).filter(
        UsuarioAtivo.id_inventario == data.id_inventario,
        UsuarioAtivo.id_grupo == data.id_grupo
    ).all()

    grupo.status = "CONCLUIDO"

    db.query(UsuarioAtivo).filter(
        UsuarioAtivo.id_inventario == data.id_inventario,
        UsuarioAtivo.id_grupo == data.id_grupo
    ).delete(synchronize_session=False)

    db.commit()

    return {
        "success": True,
        "grupo": grupo.nome,
        "id_grupo": grupo.id,
        "count": total_grupo,
        "membros_removidos": [m.usuario for m in membros],
        "finalizado_por": data.usuario,
        "forcado": False
    }





@app.post("/grupos/concluir-forcado")
def concluir_grupo_forcado(data: ConcluirGrupoIn, db: Session = Depends(get_db)):
    grupo = db.query(Grupo).filter(
        Grupo.id == data.id_grupo,
        Grupo.id_inventario == data.id_inventario
    ).first()

    if not grupo:
        raise HTTPException(status_code=404, detail="Grupo não encontrado")

    total_grupo = db.query(func.count(Bipe.id)).filter(
        Bipe.id_inventario == data.id_inventario,
        Bipe.id_grupo == data.id_grupo
    ).scalar() or 0

    membros = db.query(UsuarioAtivo).filter(
        UsuarioAtivo.id_inventario == data.id_inventario,
        UsuarioAtivo.id_grupo == data.id_grupo
    ).all()

    grupo.status = "CONCLUIDO"

    db.query(UsuarioAtivo).filter(
        UsuarioAtivo.id_inventario == data.id_inventario,
        UsuarioAtivo.id_grupo == data.id_grupo
    ).delete(synchronize_session=False)

    db.commit()

    return {
        "success": True,
        "grupo": grupo.nome,
        "id_grupo": grupo.id,
        "count": total_grupo,
        "forcado": True,
        "membros_removidos": [m.usuario for m in membros],
        "finalizado_por": data.usuario,
        "forcado": True
    }


@app.post("/grupos/editar-meta")
def editar_meta(data: EditarMetaIn, db: Session = Depends(get_db)):
    grupo = db.query(Grupo).filter(
        Grupo.id == data.id_grupo,
        Grupo.id_inventario == data.id_inventario
    ).first()

    if not grupo:
        raise HTTPException(status_code=404, detail="Grupo não encontrado")

    if int(data.nova_meta or 0) <= 0:
        raise HTTPException(status_code=400, detail="Meta inválida")

    grupo.meta = int(data.nova_meta)
    if grupo.status == "CONCLUIDO":
        grupo.status = "RESERVADO" if db.query(UsuarioAtivo).filter(
            UsuarioAtivo.id_inventario == data.id_inventario,
            UsuarioAtivo.id_grupo == data.id_grupo
        ).first() else "DISPONIVEL"
    db.commit()

    total_grupo = db.query(func.count(Bipe.id)).filter(
        Bipe.id_inventario == data.id_inventario,
        Bipe.id_grupo == data.id_grupo
    ).scalar() or 0

    return {
        "success": True,
        "grupo": grupo.nome,
        "id_grupo": grupo.id,
        "nova_meta": int(grupo.meta or 0),
        "bipes_atual": int(total_grupo)
    }




@app.post("/grupos/tornar-colaborativo")
def tornar_colaborativo(data: TornarColaborativoIn, db: Session = Depends(get_db)):
    grupo = db.query(Grupo).filter(
        Grupo.id == data.id_grupo,
        Grupo.id_inventario == data.id_inventario
    ).first()

    if not grupo:
        raise HTTPException(status_code=404, detail="Grupo não encontrado")

    vagas = int(data.vagas or 2)
    if vagas < 2:
        raise HTTPException(status_code=400, detail="Vagas inválidas")

    grupo.colaborativo = True
    grupo.vagas = vagas
    db.commit()

    membros = listar_membros_grupo(db, data.id_inventario, data.id_grupo)
    return {
        "success": True,
        "grupo": grupo.nome,
        "id_grupo": grupo.id,
        "colaborativo": True,
        "vagas": int(grupo.vagas or 0),
        "membros": [m.usuario for m in membros]
    }


@app.post("/grupos/renomear")
def renomear_grupo(data: RenomearGrupoIn, db: Session = Depends(get_db)):
    grupo = db.query(Grupo).filter(
        Grupo.id == data.id_grupo,
        Grupo.id_inventario == data.id_inventario
    ).first()

    if not grupo:
        raise HTTPException(status_code=404, detail="Grupo não encontrado")

    novo_nome = norm_txt(data.novo_nome).upper()
    if not novo_nome:
        raise HTTPException(status_code=400, detail="Novo nome inválido")

    grupo.nome = novo_nome
    db.query(UsuarioAtivo).filter(
        UsuarioAtivo.id_inventario == data.id_inventario,
        UsuarioAtivo.id_grupo == data.id_grupo
    ).update({UsuarioAtivo.grupo_nome: novo_nome}, synchronize_session=False)

    db.query(Bipe).filter(
        Bipe.id_inventario == data.id_inventario,
        Bipe.id_grupo == data.id_grupo
    ).update({Bipe.grupo_nome: novo_nome}, synchronize_session=False)

    db.commit()

    return {
        "success": True,
        "grupo": novo_nome,
        "id_grupo": grupo.id
    }


@app.post("/grupos/remover-do-grupo")
def remover_do_grupo(data: RemoverDoGrupoIn, db: Session = Depends(get_db)):
    ativo = db.query(UsuarioAtivo).filter(
        UsuarioAtivo.usuario == data.usuario,
        UsuarioAtivo.id_inventario == data.id_inventario
    ).first()

    if not ativo:
        raise HTTPException(status_code=404, detail="Usuário não está em grupo ativo")

    grupo = db.query(Grupo).filter(
        Grupo.id == ativo.id_grupo,
        Grupo.id_inventario == data.id_inventario
    ).first()

    nome_grupo = ativo.grupo_nome
    id_grupo = ativo.id_grupo
    db.delete(ativo)
    db.flush()

    restantes = listar_membros_grupo(db, data.id_inventario, id_grupo)
    if grupo:
        grupo.status = "RESERVADO" if restantes else "DISPONIVEL"

    total_grupo = contar_bipes_grupo(db, data.id_inventario, id_grupo)
    db.commit()

    return {
        "success": True,
        "usuario_removido": data.usuario,
        "grupo": nome_grupo,
        "id_grupo": id_grupo,
        "membros_restantes": [m.usuario for m in restantes],
        "bipes_mantidos": int(total_grupo)
    }


@app.post("/grupos/resetar")
def resetar_grupo(data: ResetarGrupoIn, db: Session = Depends(get_db)):
    grupo = db.query(Grupo).filter(
        Grupo.id == data.id_grupo,
        Grupo.id_inventario == data.id_inventario
    ).first()

    if not grupo:
        raise HTTPException(status_code=404, detail="Grupo não encontrado")

    usuario_no_inventario = obter_grupo_ativo_do_usuario(db, data.usuario, data.id_inventario)
    if not usuario_no_inventario:
        inventario = db.get(Inventario, data.id_inventario)
        if inventario is None:
            raise HTTPException(status_code=404, detail="Inventário não encontrado")

    membros = db.query(UsuarioAtivo).filter(
        UsuarioAtivo.id_inventario == data.id_inventario,
        UsuarioAtivo.id_grupo == data.id_grupo
    ).all()

    bipes_apagados = db.query(Bipe).filter(
        Bipe.id_inventario == data.id_inventario,
        Bipe.id_grupo == data.id_grupo
    ).delete(synchronize_session=False)

    db.query(UsuarioAtivo).filter(
        UsuarioAtivo.id_inventario == data.id_inventario,
        UsuarioAtivo.id_grupo == data.id_grupo
    ).delete(synchronize_session=False)

    grupo.status = "DISPONIVEL"

    db.commit()

    return {
        "success": True,
        "grupo": grupo.nome,
        "id_grupo": grupo.id,
        "bipes_apagados": int(bipes_apagados or 0),
        "membros_removidos": [m.usuario for m in membros]
    }


@app.get("/admin/painel")

def admin_painel(db: Session = Depends(get_db)):
    inventarios = db.query(Inventario).all()
    usuarios = db.query(UsuarioAtivo).all()
    grupos = db.query(Grupo).all()
    bipes = db.query(Bipe).all()
    itens_estoque = db.query(Estoque).filter(Estoque.ativo.is_(True)).all()

    estoque_por_ean = {str(item.ean or ""): item for item in itens_estoque}

    resumo_grupos = []
    for g in grupos:
        total = db.query(Bipe).filter(
            Bipe.id_inventario == g.id_inventario,
            Bipe.id_grupo == g.id
        ).count()

        membros = db.query(UsuarioAtivo).filter(
            UsuarioAtivo.id_inventario == g.id_inventario,
            UsuarioAtivo.id_grupo == g.id
        ).all()

        resumo_grupos.append({
            "id": g.id,
            "nome": g.nome,
            "id_inventario": g.id_inventario,
            "meta": g.meta,
            "status": g.status,
            "colaborativo": g.colaborativo,
            "membros": [m.usuario for m in membros],
            "bipes": total
        })

    bipes_out = []
    for b in bipes:
        item = estoque_por_ean.get(str(b.ean or ""))
        ref = item.produto if item else ""
        cor = item.cor_produ if item else ""
        tamanho = item.tamanho if item else ""
        grade = item.grade if item else ""
        filial = item.filial if item else ""
        ref_cor = item.ref_cor if item else ""
        bipes_out.append({
            "usuario": b.usuario,
            "id_inventario": b.id_inventario,
            "id_grupo": b.id_grupo,
            "grupo_nome": b.grupo_nome,
            "ean": b.ean,
            "hora": str(b.criado_em),
            "label_compact": f"{ref_cor} {tamanho}".strip() if item else "",
            "ref": ref,
            "cor": cor,
            "tamanho": tamanho,
            "grade": grade,
            "filial": filial,
            "ref_cor": ref_cor,
            "nao_encontrado": item is None
        })

    return {
        "success": True,
        "inventarios": [
            {"id": i.id, "nome": i.nome, "senha": i.senha, "status": i.status}
            for i in inventarios
        ],
        "usuarios_ativos": [
            {
                "usuario": u.usuario,
                "id_inventario": u.id_inventario,
                "id_grupo": u.id_grupo,
                "grupo_nome": u.grupo_nome
            } for u in usuarios
        ],
        "grupos": resumo_grupos,
        "bipes": bipes_out
    }


@app.post("/estoque/importar")
async def importar_estoque(
    arquivo: UploadFile = File(...),
    substituir_tudo: bool = Query(True),
    db: Session = Depends(get_db)
):
    nome = (arquivo.filename or "").lower()

    if not (nome.endswith(".xlsx") or nome.endswith(".csv")):
        raise HTTPException(status_code=400, detail="Envie um arquivo .xlsx ou .csv")

    linhas = []

    if nome.endswith(".xlsx"):
        conteudo = await arquivo.read()
        wb = load_workbook(io.BytesIO(conteudo), data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            raise HTTPException(status_code=400, detail="Arquivo vazio")

        cab = [norm_txt(c) for c in rows[0]]

        for row in rows[1:]:
            item = dict(zip(cab, row))
            linhas.append(item)

    elif nome.endswith(".csv"):
        conteudo = await arquivo.read()
        texto = conteudo.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(texto))
        for row in reader:
            linhas.append(row)

    obrigatorias = ["PRODUTO", "COR_PRODU", "FILIAL", "TAMANHO", "QUANTIDADE", "GRADE", "CODIGO_BARR"]
    if not linhas:
        raise HTTPException(status_code=400, detail="Nenhuma linha encontrada no arquivo")

    primeira = linhas[0]
    faltando = [c for c in obrigatorias if c not in primeira]
    if faltando:
        raise HTTPException(status_code=400, detail=f"Colunas ausentes: {', '.join(faltando)}")

    if substituir_tudo:
        db.query(Estoque).delete()
        db.commit()

    inseridos = 0
    ignorados = 0

    for item in linhas:
        produto = norm_txt(item.get("PRODUTO"))
        cor_produ = norm_txt(item.get("COR_PRODU"))
        filial = norm_txt(item.get("FILIAL"))
        tamanho = norm_txt(item.get("TAMANHO"))
        grade = norm_txt(item.get("GRADE"))
        ean = normalizar_ean(item.get("CODIGO_BARR"))
        ref_cor = montar_ref_cor(produto, cor_produ)

        try:
            quantidade = int(float(str(item.get("QUANTIDADE") or "0").replace(",", ".")))
        except Exception:
            quantidade = 0

        if not ean:
            ignorados += 1
            continue

        existente = db.query(Estoque).filter(Estoque.ean == ean).first()
        if existente:
            existente.produto = produto
            existente.cor_produ = cor_produ
            existente.filial = filial
            existente.tamanho = tamanho
            existente.quantidade = quantidade
            existente.grade = grade
            existente.ref_cor = ref_cor
            existente.ativo = True
        else:
            novo = Estoque(
                produto=produto,
                cor_produ=cor_produ,
                filial=filial,
                tamanho=tamanho,
                quantidade=quantidade,
                grade=grade,
                ean=ean,
                ref_cor=ref_cor,
                ativo=True
            )
            db.add(novo)

        inseridos += 1

    db.commit()

    total = db.query(Estoque).count()

    return {
        "success": True,
        "inseridos": inseridos,
        "ignorados": ignorados,
        "total_estoque": total
    }


@app.get("/estoque/validar")
def validar_estoque(ean: str, db: Session = Depends(get_db)):
    ean_norm = normalizar_ean(ean)
    if not ean_norm:
        return {"success": True, "encontrado": False}

    item = db.query(Estoque).filter(
        Estoque.ean == ean_norm,
        Estoque.ativo.is_(True)
    ).first()

    if not item:
        return {"success": True, "encontrado": False, "ean": ean_norm}

    return {
        "success": True,
        "encontrado": True,
        "ean": item.ean,
        "info": {
            "ref": item.produto,
            "cor": item.cor_produ,
            "grade": item.grade,
            "tamanho": item.tamanho,
            "filial": item.filial,
            "qtdEstoque": item.quantidade,
            "label": f"{item.ref_cor} {item.tamanho}".strip(),
            "labelCompact": f"{item.ref_cor} {item.tamanho}".strip(),
            "refCor": item.ref_cor
        }
    }


@app.get("/estoque/mapa-mini")
def estoque_mapa_mini(db: Session = Depends(get_db)):
    itens = db.query(Estoque).filter(Estoque.ativo.is_(True)).all()
    mapa = {}
    for item in itens:
        mapa[item.ean] = f"{item.ref_cor} {item.tamanho}".strip()

    return {
        "success": True,
        "total": len(mapa),
        "mapa": mapa
    }
