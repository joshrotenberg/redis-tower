#!/bin/bash
# Count implemented Redis commands

echo "=== Redis Tower Command Count ==="
echo ""

total=$(find src/commands -name "*.rs" -exec grep -h "^pub struct [A-Z]" {} \; | wc -l)
echo "Total commands: $total"
echo ""

echo "By category:"
for file in src/commands/*.rs; do
    name=$(basename "$file" .rs)
    count=$(grep -c "^pub struct [A-Z]" "$file" 2>/dev/null || echo 0)
    if [ "$count" -gt 0 ]; then
        printf "  %-15s %3d\n" "$name:" "$count"
    fi
done
echo ""

echo "Recently added (last 5 commits):"
git log -5 --pretty=format:"%h %s" --grep="feat:" --grep="add" -i | head -5
