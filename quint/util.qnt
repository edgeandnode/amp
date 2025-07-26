module util {
    pure def require(p: bool): bool = p

    pure def min(a: int, b: int): int = if (a < b) a else b
    pure def max(a: int, b: int): int = if (a > b) a else b

    pure def listValues(l: List[a]): Set[a] = l.indices().map(i => l[i])

    pure def setSum(s: Set[int]): int = s.fold(0, ((s, n) => s + n))

    type Option[a] = Some(a) | None
    pure def unwrapOr(o: Option[a], alt: a): a = {
        match o {
            | None => alt
            | Some(x) => x
        }
    }
    pure def setMin(s: Set[int]): Option[int] = {
        s.fold(None, (acc, n) => match acc {
            | None => Some(n)
            | Some(m) => Some(min(m, n))
        })
    }
    pure def setMax(s: Set[int]): Option[int] = {
        s.fold(None, (acc, n) => match acc {
            | None => Some(n)
            | Some(m) => Some(max(m, n))
        })
    }
}
