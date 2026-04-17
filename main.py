from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, Query
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import func, Column, Integer, String, DateTime
from database import Base, engine, SessionLocal
from models import Inventario, Grupo, UsuarioAtivo, Bipe, Estoque
from openpyxl import load_workbook, Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font
import csv
import io
import re
from collections import defaultdict
from datetime import datetime
from urllib.parse import quote
from typing import Optional


def _import_reportlab():
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas
    from reportlab.graphics.barcode import createBarcodeDrawing
    from reportlab.graphics import renderPDF
    return A4, mm, canvas, createBarcodeDrawing, renderPDF

app = FastAPI(title="HS Inventário API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

Base.metadata.create_all(bind=engine)

class EtiquetaPendente(Base):
    __tablename__ = "etiquetas_pendentes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ean = Column(String(32), nullable=False, index=True)
    ref_cor = Column(String(255), nullable=False, default="")
    grade = Column(String(64), nullable=False, default="")
    id_inventario = Column(String(64), nullable=False, index=True)
    id_grupo = Column(String(64), nullable=False, default="", index=True)
    usuario = Column(String(120), nullable=False, default="")
    criado_em = Column(DateTime, nullable=False, default=datetime.utcnow)


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


class ManualBipeIn(BaseModel):
    usuario: str
    id_inventario: str
    id_grupo: str
    ean: str
    secao: Optional[str] = "MANUAL"


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




def buscar_item_estoque_por_ean(db: Session, ean: str):
    ean_norm = normalizar_ean(ean)
    if not ean_norm:
        return None

    candidatos = db.query(Estoque).filter(Estoque.ativo.is_(True)).all()
    for item in candidatos:
        if normalizar_ean(getattr(item, "ean", "")) == ean_norm:
            return item
    return None


def registrar_etiqueta_manual(db: Session, *, ean: str, id_inventario: str, id_grupo: str, usuario: str):
    item = buscar_item_estoque_por_ean(db, ean)
    if not item:
        return None

    ref_cor = norm_txt(getattr(item, "ref_cor", "")) or montar_ref_cor(getattr(item, "produto", ""), getattr(item, "cor_produ", ""))
    grade = norm_txt(getattr(item, "tamanho", ""))

    etiqueta = EtiquetaPendente(
        ean=normalizar_ean(ean),
        ref_cor=ref_cor,
        grade=grade,
        id_inventario=norm_txt(id_inventario),
        id_grupo=norm_txt(id_grupo),
        usuario=norm_txt(usuario),
    )
    db.add(etiqueta)
    db.commit()
    db.refresh(etiqueta)
    return etiqueta


def listar_etiquetas_pendentes(db: Session, id_inventario: str = ""):
    query = db.query(EtiquetaPendente).order_by(EtiquetaPendente.id.asc())
    if norm_txt(id_inventario):
        query = query.filter(EtiquetaPendente.id_inventario == norm_txt(id_inventario))
    rows = query.all()
    return [
        {
            "id": row.id,
            "ean": norm_txt(row.ean),
            "refCor": norm_txt(row.ref_cor),
            "grade": norm_txt(row.grade),
            "data": row.criado_em.isoformat() if row.criado_em else "",
            "idInventario": norm_txt(row.id_inventario),
            "idGrupo": norm_txt(row.id_grupo),
            "usuario": norm_txt(row.usuario),
        }
        for row in rows
    ]


def gerar_pdf_etiquetas_bytes(etiquetas):
    try:
        A4, mm, canvas, createBarcodeDrawing, renderPDF = _import_reportlab()
    except Exception as e:
        raise RuntimeError("Biblioteca reportlab não está instalada no servidor. Instale com: pip install reportlab") from e

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    page_w, page_h = A4

    cols = 4
    rows = 4
    margin_x = 8 * mm
    margin_y = 8 * mm
    gap_x = 4 * mm
    gap_y = 4 * mm
    label_w = (page_w - (2 * margin_x) - ((cols - 1) * gap_x)) / cols
    label_h = (page_h - (2 * margin_y) - ((rows - 1) * gap_y)) / rows

    def draw_label(x, y, e):
        c.setLineWidth(0.8)
        c.rect(x, y, label_w, label_h)

        grade_w = 14 * mm
        grade_h = 14 * mm
        c.rect(x + 3 * mm, y + label_h - grade_h - 3 * mm, grade_w, grade_h)
        c.setFont('Helvetica-Bold', 10)
        c.drawCentredString(x + 3 * mm + grade_w / 2, y + label_h - 10 * mm, norm_txt(e.get('grade')) or '?')

        ref_cor = norm_txt(e.get('refCor')) or '—'
        c.setFont('Helvetica-Bold', 6.8)
        text = c.beginText(x + 3 * mm + grade_w + 3 * mm, y + label_h - 7 * mm)
        max_chars = 18
        for part in [ref_cor[i:i+max_chars] for i in range(0, min(len(ref_cor), max_chars*2), max_chars)]:
            text.textLine(part)
        c.drawText(text)

        ean = normalizar_ean(e.get('ean'))
        if len(ean) == 13:
            try:
                barcode = createBarcodeDrawing('EAN13', value=ean, humanReadable=False, barHeight=12*mm, width=label_w-10*mm)
                renderPDF.draw(barcode, c, x + 5*mm, y + 16*mm)
            except Exception:
                c.setFont('Helvetica', 8)
                c.drawString(x + 5*mm, y + 24*mm, ean)
        else:
            c.setFont('Helvetica', 8)
            c.drawString(x + 5*mm, y + 24*mm, ean)

        c.setFont('Helvetica', 7.5)
        c.drawCentredString(x + label_w/2, y + 8*mm, ean)

    for idx, e in enumerate(etiquetas):
        page_pos = idx % (cols * rows)
        col = page_pos % cols
        row = page_pos // cols
        x = margin_x + col * (label_w + gap_x)
        y = page_h - margin_y - (row + 1) * label_h - row * gap_y
        draw_label(x, y, e)
        if page_pos == (cols * rows) - 1 and idx != len(etiquetas) - 1:
            c.showPage()

    if not etiquetas:
        c.setFont('Helvetica', 12)
        c.drawString(20 * mm, page_h - 20 * mm, 'Nenhuma etiqueta pendente.')

    c.save()
    buffer.seek(0)
    return buffer.getvalue()


def montar_label_estoque(produto: str, cor: str, tamanho: str) -> str:
    partes = [norm_txt(produto), norm_txt(cor), norm_txt(tamanho)]
    return " ".join([p for p in partes if p]).strip()


def agregar_estoque_por_ean(db: Session):
    itens = db.query(Estoque).filter(Estoque.ativo.is_(True)).all()
    estoque = {}
    total_estoque = 0

    for item in itens:
        ean = normalizar_ean(item.ean)
        if not ean:
            continue

        qtd = int(item.quantidade or 0)
        total_estoque += qtd

        if ean not in estoque:
            estoque[ean] = {
                "ean": ean,
                "qtdEstoque": 0,
                "label": montar_label_estoque(item.produto, item.cor_produ, item.tamanho),
                "ref": norm_txt(item.produto),
                "cor": norm_txt(item.cor_produ),
                "grade": norm_txt(item.tamanho),
                "tamanho": norm_txt(item.tamanho),
                "refCor": norm_txt(item.ref_cor),
                "filial": norm_txt(item.filial),
            }

        estoque[ean]["qtdEstoque"] += qtd
        if not estoque[ean]["label"]:
            estoque[ean]["label"] = montar_label_estoque(item.produto, item.cor_produ, item.tamanho)

    return estoque, total_estoque


def agregar_bipes_por_ean(db: Session, id_inventario: str):
    query = db.query(Bipe)
    if id_inventario:
        query = query.filter(Bipe.id_inventario == id_inventario)
    rows = query.all()

    bipados = defaultdict(int)
    total_consolidado = 0
    for row in rows:
        ean = normalizar_ean(row.ean)
        if not ean:
            continue
        bipados[ean] += 1
        total_consolidado += 1
    return bipados, total_consolidado


def montar_confronto_estoque(db: Session, id_inventario: str):
    estoque, total_estoque = agregar_estoque_por_ean(db)
    bipados, total_consolidado = agregar_bipes_por_ean(db, id_inventario)

    acima = []
    proximo = []
    abaixo50 = []
    exato = []
    nao_encontrados = []

    for ean, item_base in estoque.items():
        qtd_bipada = int(bipados.get(ean, 0))
        if qtd_bipada == 0:
            continue

        item = {
            "ean": ean,
            "label": item_base.get("label", ""),
            "ref": item_base.get("ref", ""),
            "cor": item_base.get("cor", ""),
            "grade": item_base.get("grade", ""),
            "tamanho": item_base.get("tamanho", ""),
            "refCor": item_base.get("refCor", ""),
            "filial": item_base.get("filial", ""),
            "qtdEstoque": int(item_base.get("qtdEstoque", 0)),
            "qtdBipada": qtd_bipada,
        }

        if qtd_bipada > item["qtdEstoque"]:
            acima.append(item)
        elif qtd_bipada == item["qtdEstoque"]:
            exato.append(item)
        else:
            pct = 100 if item["qtdEstoque"] == 0 else round((qtd_bipada / item["qtdEstoque"]) * 100)
            item_pct = dict(item)
            item_pct["pct"] = int(pct)
            if pct >= 50:
                proximo.append(item_pct)
            else:
                abaixo50.append(item_pct)

    for ean, qtd_bipada in bipados.items():
        if ean in estoque:
            continue
        nao_encontrados.append({
            "ean": ean,
            "qtdBipada": int(qtd_bipada),
            "qtdEstoque": 0,
            "label": "",
            "ref": "",
            "cor": "",
            "grade": "",
            "tamanho": "",
            "refCor": "",
            "filial": "",
        })

    proximo.sort(key=lambda x: (-int(x.get("pct", 0)), str(x.get("label", ""))))
    abaixo50.sort(key=lambda x: (-int(x.get("pct", 0)), str(x.get("label", ""))))
    acima.sort(key=lambda x: -((int(x.get("qtdBipada", 0)) - int(x.get("qtdEstoque", 0)))))
    exato.sort(key=lambda x: str(x.get("label", "")))
    nao_encontrados.sort(key=lambda x: (-int(x.get("qtdBipada", 0)), str(x.get("ean", ""))))

    percentual = round((total_consolidado / total_estoque) * 1000) / 10 if total_estoque > 0 else 0

    return {
        "success": True,
        "acima": acima,
        "proximo": proximo,
        "abaixo50": abaixo50,
        "exato": exato,
        "naoEncontrados": nao_encontrados,
        "totalEstoque": int(total_estoque),
        "totalBipados": len(bipados),
        "totalConsolidado": int(total_consolidado),
        "percentualConsolidado": percentual,
    }




@app.get("/estoque/validar")
def validar_estoque_por_ean(ean: str = "", db: Session = Depends(get_db)):
    ean_norm = normalizar_ean(ean)
    if not ean_norm:
        return {
            "success": True,
            "encontrado": False,
            "ean": "",
            "item": None,
            "message": "EAN não informado"
        }

    item = buscar_item_estoque_por_ean(db, ean_norm)
    if not item:
        return {
            "success": True,
            "encontrado": False,
            "ean": ean_norm,
            "item": None
        }

    ref = norm_txt(getattr(item, "produto", ""))
    cor = norm_txt(getattr(item, "cor_produ", ""))
    tamanho = norm_txt(getattr(item, "tamanho", ""))
    ref_cor = norm_txt(getattr(item, "ref_cor", "")) or montar_ref_cor(ref, cor)

    return {
        "success": True,
        "encontrado": True,
        "ean": ean_norm,
        "item": {
            "ean": ean_norm,
            "ref": ref,
            "cor": cor,
            "tamanho": tamanho,
            "grade": tamanho,
            "refCor": ref_cor,
            "filial": norm_txt(getattr(item, "filial", "")),
            "qtdEstoque": int(getattr(item, "quantidade", 0) or 0),
            "labelCompact": f"{ref_cor} {tamanho}".strip(),
            "label": f"{ref_cor} {tamanho}".strip(),
        }
    }


@app.get("/estoque/mapa-mini")
def estoque_mapa_mini(db: Session = Depends(get_db)):
    itens = db.query(Estoque).filter(Estoque.ativo.is_(True)).all()
    mapa = {}
    total = 0

    for item in itens:
        ean = normalizar_ean(getattr(item, "ean", ""))
        if not ean:
            continue

        ref = norm_txt(getattr(item, "produto", ""))
        cor = norm_txt(getattr(item, "cor_produ", ""))
        tamanho = norm_txt(getattr(item, "tamanho", ""))
        ref_cor = norm_txt(getattr(item, "ref_cor", "")) or montar_ref_cor(ref, cor)
        label = f"{ref_cor} {tamanho}".strip()

        mapa[ean] = label or ean
        total += 1

    return {
        "success": True,
        "mapa": mapa,
        "total": total,
        "geradoEm": datetime.utcnow().isoformat()
    }

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


@app.post("/bipes/manual")
def registrar_bipe_manual(data: ManualBipeIn, db: Session = Depends(get_db)):
    grupo = db.query(Grupo).filter(
        Grupo.id == data.id_grupo,
        Grupo.id_inventario == data.id_inventario
    ).first()

    if not grupo:
        raise HTTPException(status_code=404, detail="Grupo não encontrado")

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
        ean=normalizar_ean(data.ean)
    )
    db.add(registro)
    db.commit()
    db.refresh(registro)

    etiqueta = registrar_etiqueta_manual(
        db,
        ean=data.ean,
        id_inventario=data.id_inventario,
        id_grupo=data.id_grupo,
        usuario=data.usuario,
    )

    total_grupo = db.query(Bipe).filter(
        Bipe.id_inventario == data.id_inventario,
        Bipe.id_grupo == data.id_grupo
    ).count()

    return {
        "success": True,
        "total_grupo": total_grupo,
        "etiqueta_gerada": bool(etiqueta),
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


@app.get("/estoque/confronto")
def confrontar_estoque(id_inventario: str = "", db: Session = Depends(get_db)):
    return montar_confronto_estoque(db, norm_txt(id_inventario))


@app.get("/estoque/confronto/relatorio")
def gerar_relatorio_confronto(id_inventario: str = "", db: Session = Depends(get_db)):
    confronto = montar_confronto_estoque(db, norm_txt(id_inventario))
    if not confronto.get("success"):
        raise HTTPException(status_code=400, detail=confronto.get("message") or "Falha ao gerar relatório")

    wb = Workbook()
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
    inv = norm_txt(id_inventario) or "—"

    def montar_sheet(ws, titulo, cabecalho, linhas):
        bloco = []
        bloco.append(["RELATÓRIO DE CONFRONTO DE ESTOQUE", "", "", "", "", ""])
        bloco.append(["Inventário:", inv, "Gerado em:", agora, "", ""])
        bloco.append(["", "", "", "", "", ""])
        bloco.append([titulo, "", "", "", "", ""])
        bloco.append(cabecalho)
        if linhas:
            bloco.extend(linhas)
        else:
            bloco.append(["(nenhum)", "", "", "", "", ""])
        for row in bloco:
            ws.append(row)
        ws["A1"].font = Font(bold=True, size=12)
        for col in range(1, 7):
            ws.column_dimensions[get_column_letter(col)].width = 18

    ws_acima = wb.active
    ws_acima.title = "ACIMA"
    montar_sheet(
        ws_acima,
        f"ACIMA DO ESTOQUE ({len(confronto['acima'])} itens)",
        ["Referência", "Cor", "Tamanho", "Qtd Estoque", "Qtd Bipada", "Diferença"],
        [[i.get("ref", ""), i.get("cor", ""), i.get("grade", ""), int(i.get("qtdEstoque", 0)), int(i.get("qtdBipada", 0)), int(i.get("qtdBipada", 0)) - int(i.get("qtdEstoque", 0))] for i in confronto["acima"]]
    )

    ws_faltando = wb.create_sheet(title="FALTANDO")
    montar_sheet(
        ws_faltando,
        f"PRÓXIMO / FALTANDO ({len(confronto['proximo'])} itens)",
        ["Referência", "Cor", "Tamanho", "Qtd Estoque", "Qtd Bipada", "% Bipado"],
        [[i.get("ref", ""), i.get("cor", ""), i.get("grade", ""), int(i.get("qtdEstoque", 0)), int(i.get("qtdBipada", 0)), f"{i.get('pct', 0)}%"] for i in confronto["proximo"]]
    )

    ws_abaixo50 = wb.create_sheet(title="ABAIXO50")
    montar_sheet(
        ws_abaixo50,
        f"ABAIXO DE 50% ({len(confronto['abaixo50'])} itens)",
        ["Referência", "Cor", "Tamanho", "Qtd Estoque", "Qtd Bipada", "% Bipado"],
        [[i.get("ref", ""), i.get("cor", ""), i.get("grade", ""), int(i.get("qtdEstoque", 0)), int(i.get("qtdBipada", 0)), f"{i.get('pct', 0)}%"] for i in confronto["abaixo50"]]
    )

    ws_exato = wb.create_sheet(title="EXATO")
    montar_sheet(
        ws_exato,
        f"EXATO ({len(confronto['exato'])} itens)",
        ["Referência", "Cor", "Tamanho", "Qtd Estoque", "Qtd Bipada", "Diferença"],
        [[i.get("ref", ""), i.get("cor", ""), i.get("grade", ""), int(i.get("qtdEstoque", 0)), int(i.get("qtdBipada", 0)), 0] for i in confronto["exato"]]
    )

    ws_ne = wb.create_sheet(title="NAO_ENCONTRADOS")
    montar_sheet(
        ws_ne,
        f"NAO ENCONTRADOS NO ESTOQUE ({len(confronto['naoEncontrados'])} itens)",
        ["EAN", "Referência", "Cor", "Tamanho", "Qtd Estoque", "Qtd Bipada"],
        [[i.get("ean", ""), i.get("ref", ""), i.get("cor", ""), i.get("grade", ""), int(i.get("qtdEstoque", 0)), int(i.get("qtdBipada", 0))] for i in confronto["naoEncontrados"]]
    )

    buf = io.BytesIO()
    wb.save(buf)
    conteudo = buf.getvalue()
    nome_base = f"relatorio_confronto_{inv or 'geral'}_{datetime.now().strftime('%d%m%Y_%H%M%S')}.xlsx"
    nome_seguro = re.sub(r'[^A-Za-z0-9._-]+', '_', nome_base)
    headers = {
        "Content-Disposition": f"attachment; filename=\"{nome_seguro}\"; filename*=UTF-8''{quote(nome_base)}",
        "Access-Control-Expose-Headers": "Content-Disposition",
        "Cache-Control": "no-store"
    }
    return Response(
        content=conteudo,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )
