module util {
    pure def require(p: bool): bool = p

    pure def min(a: int, b: int): int = if (a < b) a else b
    pure def max(a: int, b: int): int = if (a > b) a else b

    pure def listValues(l: List[a]): Set[a] = l.indices().map(i => l[i])

    pure def setSum(s: Set[int]): int = s.fold(0, ((s, n) => s + n))
}
